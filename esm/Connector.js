import _ from 'lodash';
import request from 'request-promise-native';
import mqtt from 'async-mqtt';

async function deviceExists(url, headers, id) {
  try {
    await request.get({ url: `${url}/${id}`, headers, json: true });
  } catch (error) {
    if (error.response.statusCode === 404) {
      return false;
    }
  }

  return true;
}

async function serviceExists(url, headers) {
  const service = await request.get({ url, headers, json: true });
  return service.count > 0;
}

async function createService(iotAgentUrl, orionUrl, servicePath, apiKey, entityType) {
  const url = `${iotAgentUrl}/iot/services`;
  const service = {
    name: 'knot',
    resource: '/iot/d',
  };
  const headers = {
    'fiware-service': service.name,
    'fiware-servicepath': servicePath,
  };

  if (await serviceExists(url, headers)) {
    return;
  }

  service.entity_type = entityType;
  service.apikey = apiKey;
  service.cbroker = orionUrl;

  if (entityType === 'device') {
    service.commands = [
      {
        name: 'setConfig',
        type: 'command',
      },
      {
        name: 'setProperties',
        type: 'command',
      },
    ];
  } else if (entityType === 'sensor') {
    service.attributes = [{
      name: 'value',
      type: 'string',
    }];
    service.commands = [
      {
        name: 'setData',
        type: 'command',
      },
      {
        name: 'getData',
        type: 'command',
      },
    ];
  }

  await request.post({
    url, headers, body: { services: [service] }, json: true,
  });
}

function mapDeviceToFiware(device) {
  return {
    device_id: device.id,
    entity_name: device.id,
    entity_type: 'device',
    protocol: 'IoTA-UL',
    transport: 'MQTT',
    static_attributes: [{
      name: 'name',
      type: 'string',
      value: device.name,
    }],
  };
}

function mapSensorToFiware(id, schema) {
  const schemaList = _.map(schema, (value, key) => ({ name: key, type: typeof value, value }));

  return {
    device_id: schema.sensor_id.toString(),
    entity_name: schema.sensor_id.toString(),
    entity_type: 'sensor',
    protocol: 'IoTA-UL',
    transport: 'MQTT',
    static_attributes: [{
      name: 'device',
      type: 'string',
      value: id,
    }].concat(schemaList),
  };
}

function mapSensorFromFiware(device) {
  const schema = {};

  schema.sensor_id = parseInt(device.device_id, 10);

  device.static_attributes.forEach((attr) => {
    if (attr.name === 'value_type') {
      schema.value_type = attr.value;
    } else if (attr.name === 'unit') {
      schema.unit = attr.value;
    } else if (attr.name === 'type_id') {
      schema.type_id = attr.value;
    } else if (attr.name === 'name') {
      schema.name = attr.value;
    }
  });

  return schema;
}

function parseULValue(value) {
  if (value.indexOf('=') === -1) {
    return value;
  }

  const objValue = {};
  const attrs = value.split('|');

  attrs.forEach((attr) => {
    objValue[attr.slice(0, attr.indexOf('='))] = attr.slice(attr.indexOf('=') + 1, attr.length);
  });

  return objValue;
}

function parseULMessage(topic, message) {
  const apiKey = topic.split('/')[1];
  const entityId = message.slice(0, message.indexOf('@'));
  const command = message.slice(message.indexOf('@') + 1, message.indexOf('|'));
  const value = parseULValue(message.slice(message.indexOf('|') + 1, message.length));

  const id = apiKey === 'default' ? entityId : apiKey;

  return {
    id,
    entityId,
    command,
    value,
  };
}

async function removeDeviceFromIoTAgent(iotAgentUrl, serviceConfig, id) {
  let url = `${iotAgentUrl}/iot/devices/${id}`;
  const headers = {
    'fiware-service': 'knot',
    'fiware-servicepath': '/device',
  };

  await request.delete({ url, headers, json: true });

  headers['fiware-servicepath'] = `/device/${id}`;
  url = `${iotAgentUrl}/iot/devices`;
  const sensors = await request.get({ url, headers, json: true });
  if (sensors.count === 0) {
    return;
  }

  const promises = sensors.devices.map(async (sensor) => {
    url = `${iotAgentUrl}/iot/devices/${sensor.device_id}`;
    await request.delete({ url, headers, json: true });
  });

  await Promise.all(promises);

  const { resource } = serviceConfig;
  url = `${iotAgentUrl}/iot/services/?resource=${resource}&apikey=${id}`;
  await request.delete({ url, headers, json: true });
}

async function removeDeviceFromOrion(orionUrl, id) {
  let url = `${orionUrl}/v2/entities/${id}`;
  const headers = {
    'fiware-service': 'knot',
    'fiware-servicepath': '/device',
  };

  await request.delete({ url, headers, json: true });

  headers['fiware-servicepath'] = `/device/${id}`;
  url = `${orionUrl}/v2/entities`;
  const sensors = await request.get({ url, headers, json: true });
  if (sensors.length === 0) {
    return;
  }

  const promises = sensors.map(async (sensor) => {
    url = `${orionUrl}/v2/entities/${sensor.id}`;
    await request.delete({ url, headers, json: true });
  });

  await Promise.all(promises);
}

class Connector {
  constructor(settings) {
    this.iotAgentUrl = `http://${settings.iota.hostname}:${settings.iota.port}`;
    this.orionUrl = `http://${settings.orion.hostname}:${settings.orion.port}`;
    this.iotAgentMQTT = `mqtt://${settings.iota.hostname}`;
  }

  async start() {
    this.onDataUpdatedCb = _.noop();
    this.onDataRequestedCb = _.noop();
    this.onConfigUpdatedCb = _.noop();
    this.onPropertiesUpdatedCb = _.noop();

    await createService(this.iotAgentUrl, this.orionUrl, '/device', 'default', 'device');

    return new Promise((resolve, reject) => {
      this.client = mqtt.connect(this.iotAgentMQTT);

      this.client.on('connect', () => {
        this.client.on('message', async (topic, payload) => {
          await this.messageHandler(topic, payload);
        });
        return resolve();
      });
      this.client.on('error', error => reject(error));
    });
  }

  async messageHandler(topic, payload) {
    const message = parseULMessage(topic.toString(), payload.toString());
    if (message.command === 'setData') {
      await this.handleSetData(topic, payload, message);
    } else if (message.command === 'getData') {
      await this.handleGetData(topic, payload, message);
    } else if (message.command === 'setConfig') {
      await this.handleSetConfig(topic, message);
    } else if (message.command === 'setProperties') {
      await this.handleSetProperties(topic, message);
    }
  }

  async handleSetData(topic, payload, message) {
    await this.client.publish(`${topic}exe`, payload);
    this.onDataUpdatedCb(message.id, parseInt(message.entityId, 10), message.value);
  }

  async handleGetData(topic, payload, message) {
    await this.client.publish(`${topic}exe`, payload);
    this.onDataRequestedCb(message.id, parseInt(message.entityId, 10));
  }

  async handleSetConfig(topic, ulMessage) {
    const requiredProperties = ['sensor_id', 'event_flags', 'time_sec'];
    const message = ulMessage;
    const configKeys = Object.keys(message.value);

    if (!requiredProperties.every(val => configKeys.includes(val))) {
      const response = 'The following properties are required: sensor_id, event_flags and time_sec';
      await this.client.publish(`${topic}exe`, `${message.id}@setConfig|${response}`);
      return;
    }

    _.forEach(message.value, (value, key) => {
      const intValue = parseInt(value, 10);
      message.value[key] = !Number.isNaN(intValue) && Number.isFinite(intValue) ? intValue : value;
    });

    await this.client.publish(`${topic}exe`, `${message.id}@setConfig|`);

    this.onConfigUpdatedCb(message.id, [message.value]);
  }

  async handleSetProperties(topic, message) {
    await this.client.publish(`${topic}exe`, `${message.id}@setProperties|`);
    this.onPropertiesUpdatedCb({ id: message.id, properties: message.value });
  }

  async addDevice(device) {
    const url = `${this.iotAgentUrl}/iot/devices`;
    const headers = {
      'fiware-service': 'knot',
      'fiware-servicepath': '/device',
    };

    if (await deviceExists(url, headers, device.id)) {
      return;
    }

    const fiwareDevice = mapDeviceToFiware(device);

    await request.post({
      url, headers, body: { devices: [fiwareDevice] }, json: true,
    });
    await createService(this.iotAgentUrl, this.orionUrl, `/device/${device.id}`, device.id, 'sensor');
    await this.client.subscribe(`/default/${device.id}/cmd`);
  }

  async removeDevice(id) {
    await removeDeviceFromIoTAgent(this.iotAgentUrl, this.serviceConfig, id);
    await removeDeviceFromOrion(this.orionUrl, id);
  }

  async listDevices() {
    const url = `${this.iotAgentUrl}/iot/devices`;
    const headers = {
      'fiware-service': 'knot',
      'fiware-servicepath': '/device',
    };

    const devices = await request.get({ url, headers, json: true });
    if (devices.count === 0) {
      return [];
    }

    return Promise.all(devices.devices.map(async (device) => {
      const name = device.static_attributes.find(obj => obj.name === 'name').value;

      headers['fiware-servicepath'] = `/device/${device.device_id}`;
      const sensors = await request.get({ url, headers, json: true });

      const schemaList = sensors.devices.map(sensor => mapSensorFromFiware(sensor));

      return { id: device.device_id, name, schema: schemaList };
    }));
  }

  // Device (fog) to cloud

  async publishData(id, dataList) {
    const promises = dataList.map(async (data) => {
      await this.client.publish(`/${id}/${data.sensor_id}/attrs/value`, data.value);
    });

    await Promise.all(promises);
  }

  async updateSchema(id, schemaList) {
    const url = `${this.iotAgentUrl}/iot/devices`;
    const headers = {
      'fiware-service': 'knot',
      'fiware-servicepath': `/device/${id}`,
    };

    const sensors = await Promise.all(schemaList.map(async (schema) => {
      await this.client.subscribe(`/${id}/${schema.sensor_id}/cmd`);
      return mapSensorToFiware(id, schema);
    }));

    await request.post({
      url, headers, body: { devices: sensors }, json: true,
    });
  }

  async updateProperties(id, properties) {
    const url = `${this.iotAgentUrl}/iot/devices/${id}`;
    const headers = {
      'fiware-service': 'knot',
      'fiware-servicepath': '/device',
    };

    const property = Object.keys(properties)[0];
    const value = properties[property];

    const attribute = {
      name: property,
      type: typeof value,
    };

    await request.put({
      url, headers, body: { attributes: [attribute] }, json: true,
    });
    await this.client.publish(`/default/${id}/attrs/${property}`, value.toString());
  }

  // Cloud to device (fog)

  // cb(event) where event is { id, config: {} }
  async onConfigUpdated(cb) {
    this.onConfigUpdatedCb = cb;
  }

  // cb(event) where event is { id, properties: {} }
  async onPropertiesUpdated(cb) {
    this.onPropertiesUpdatedCb = cb;
  }

  // cb(event) where event is { id, sensorId }
  async onDataRequested(cb) {
    this.onDataRequestedCb = cb;
  }

  // cb(event) where event is { id, sensorId, data }
  async onDataUpdated(cb) {
    this.onDataUpdatedCb = cb;
  }
}

export { Connector }; // eslint-disable-line import/prefer-default-export
