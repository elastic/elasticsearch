package org.elasticsearch.gradle.test

import org.gradle.api.InvalidUserDataException

/** A container for commands to run before starting an integration test cluster. */
class ClusterSetupConfiguration {
    LinkedHashMap<String, String[]> commands = new LinkedHashMap<>()

    void run(Map<String, Object> props) {
        String name = props.remove('name')
        if (name == null) {
            throw new InvalidUserDataException('name is a required parameter for a startup run command')
        }
        String[] args = props.remove('args')
        if (args == null) {
            throw new InvalidUserDataException("Either command or args must be specified for a startup ${name}")
        }
        commands.put(name, args)
    }
}
