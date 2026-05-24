/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal

import spock.lang.Specification

import org.gradle.api.artifacts.Configuration
import org.gradle.api.model.ObjectFactory
import org.gradle.api.provider.Property

class JdkSpec extends Specification {

    def "jdk version is parsed correctly"() {
        given:
        Jdk jdk = newJdk()

        when:
        jdk.setVersion(version)
        then:
        jdk.getBaseVersion() == baseVersion
        jdk.getBuild() == buildNumber

        where:
        version    | baseVersion | major | buildNumber
        "25-ea+30" | "25-ea+30"  | "25"  | "30"
        "26-ea+6"  | "26-ea+6"   | "26"  | "6"
    }

    Object newJdk(String name = "jdk") {
        Configuration configuration = Mock()
        _ * configuration.getName() >> name + "Config"

        ObjectFactory objectFactory = Mock()
        Property<String> stringProperty = Mock()
        _ * objectFactory.property(String.class) >> stringProperty

        return new Jdk(name, configuration, objectFactory)
    }
}
