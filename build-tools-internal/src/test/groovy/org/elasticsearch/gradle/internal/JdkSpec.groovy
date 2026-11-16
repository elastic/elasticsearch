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
        jdk.getMajor() == major
        jdk.getBuild() == buildNumber
        jdk.getHash() == hash

        where:
        version                                         | baseVersion  | major | buildNumber | hash
        "25-ea+30"                                      | "25-ea+30"   | "25"  | "30"        | null
        "26-ea+6"                                       | "26-ea+6"    | "26"  | "6"         | null
        "27+35@55ce5470a6294008af0057ff4626d0e5"        | "27"         | "27"  | "35"        | "55ce5470a6294008af0057ff4626d0e5"
        "26.0.2+10@818d462d89b645c7a1aad49066c454e5"    | "26.0.2"     | "26"  | "10"        | "818d462d89b645c7a1aad49066c454e5"
        "26.0.2.1+1@0123456789abcdef0123456789abcdef"   | "26.0.2.1"   | "26"  | "1"         | "0123456789abcdef0123456789abcdef"
        "27.0.1.0.2+1@924dbdccfc234bb9b9355d7c6b66034b" | "27.0.1.0.2" | "27"  | "1"         | "924dbdccfc234bb9b9355d7c6b66034b"
        "11.0.2+33"                                     | "11.0.2"     | "11"  | "33"        | null
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
