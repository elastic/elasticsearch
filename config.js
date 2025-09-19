const fs = require('fs')
const os = require('os')
const path = require('path')

function retrieveDockerCredentials() {
	try {
		const dockerConfig = JSON.parse(fs.readFileSync(path.join(os.homedir(), '.docker', 'config.json')))

		const creds = Buffer.from(dockerConfig.auths['docker.elastic.co'].auth, 'base64').toString().split(':');
		if (creds) {
			console.log("Found credentials for `docker.elastic.co`", { username: creds[0], passwordLength: creds[1].length })
			return creds
		}
	} catch (e) {
		console.warn("Failed to retrieve local Docker credentials for `docker.elastic.co`. This is fine if you don't want to validate internal Docker images can be updated", { error: e })
	}
}

const creds = retrieveDockerCredentials()

const config = {
}


if (creds) {
	config["hostRules"] = [
		{
			hostType: 'docker',
			matchHost: 'docker.elastic.co',
			username: creds[0],
			password: creds[1],
			timeout: 10_000,
		}
	]
}

module.exports = config;
