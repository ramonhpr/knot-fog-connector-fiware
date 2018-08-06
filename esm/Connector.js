class Connector {
  async start() { // eslint-disable-line no-empty-function
  }

  async addDevice(device) { // eslint-disable-line no-empty-function,no-unused-vars
  }

  async removeDevice(id) { // eslint-disable-line no-empty-function,no-unused-vars
  }

  async listDevices() { // eslint-disable-line no-empty-function,no-unused-vars
  }

  // Device (fog) to cloud

  async publishData(id, data) { // eslint-disable-line no-empty-function,no-unused-vars
  }

  async updateSchema(id, schema) { // eslint-disable-line no-empty-function,no-unused-vars
  }

  async updateProperties(id, properties) { // eslint-disable-line no-empty-function,no-unused-vars
  }

  // Cloud to device (fog)

  // cb(event) where event is { id, config: {} }
  onConfigUpdated(cb) { // eslint-disable-line no-empty-function,no-unused-vars
  }

  // cb(event) where event is { id, properties: {} }
  onPropertiesUpdated(cb) { // eslint-disable-line no-empty-function,no-unused-vars
  }

  // cb(event) where event is { id, sensorId }
  onDataRequested(cb) { // eslint-disable-line no-empty-function,no-unused-vars
  }

  // cb(event) where event is { id, sensorId, data }
  onDataUpdated(cb) { // eslint-disable-line no-empty-function,no-unused-vars
  }
}

export { Connector }; // eslint-disable-line import/prefer-default-export
