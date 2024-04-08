/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle

import spock.lang.Specification
import spock.lang.Unroll

@Unroll
class VersionSpec extends Specification {

    def "parses version string #input with mode #mode"() {
        expect:
        assertVersionEquals(input, expectedMajor, expectedMinor, expectedRevision, mode)

        where:
        input                  | mode                 | expectedMajor | expectedMinor | expectedRevision
        "7.0.1"                | Version.Mode.STRICT  | 7             | 0             | 1
        "7.0.1-alpha2"         | Version.Mode.STRICT  | 7             | 0             | 1
        "5.1.2-rc3"            | Version.Mode.STRICT  | 5             | 1             | 2
        "6.1.2-SNAPSHOT"       | Version.Mode.STRICT  | 6             | 1             | 2
        "17.03.11"             | Version.Mode.STRICT  | 17            | 3             | 11
        "6.1.2"                | Version.Mode.RELAXED | 6             | 1             | 2
        "6.1.2-SNAPSHOT"       | Version.Mode.RELAXED | 6             | 1             | 2
        "6.1.2-beta1-SNAPSHOT" | Version.Mode.RELAXED | 6             | 1             | 2
        "6.1.2-foo"            | Version.Mode.RELAXED | 6             | 1             | 2
        "6.1.2-foo-bar"        | Version.Mode.RELAXED | 6             | 1             | 2
        "16.01.22"             | Version.Mode.RELAXED | 16            | 1             | 22
        "20.10.10+dfsg1"       | Version.Mode.RELAXED | 20            | 10            | 10
        "2.15"                 | Version.Mode.RELAXED | 2             | 15            | 0
    }

    def "can use before for comparing"() {
        expect:
        Version.fromString("1.10.20").before("2.0.0")
        Version.fromString("2.0.0").before("1.10.20") == false
    }

    def "can compare alpha"() {
        expect:
        Version.fromString("7.0.0-alpha1") == Version.fromString("7.0.0-alpha1")
    }

    def "can compare snapshots"() {
        expect:
        Version.fromString("7.0.0-SNAPSHOT") == Version.fromString("7.0.0-SNAPSHOT")
    }

    def "implements readable toString"() {
        expect:
        "7.0.1" == new Version(7, 0, 1).toString()
    }

    def "can compare"() {
        expect:
         0 == new Version(7, 0, 0).compareTo(new Version(7, 0, 0))
        -1 == Version.fromString("19.0.1").compareTo(Version.fromString("20.0.3"))
         1 == Version.fromString("20.0.3").compareTo(Version.fromString("19.0.1"))
    }

    def "handles invalid version parsing"() {
        when:
        Version.fromString("")
        then:
        def e = thrown(IllegalArgumentException)
        assert e.message == "Invalid version format: ''. Should be major.minor.revision[-(alpha|beta|rc)Number|-SNAPSHOT]"

        when:
        Version.fromString("foo.bar.baz")
        then:
        e = thrown(IllegalArgumentException)
        assert e.message == "Invalid version format: 'foo.bar.baz'. Should be major.minor.revision[-(alpha|beta|rc)Number|-SNAPSHOT]"
    }

    def "handles qualifier"() {
        when:
        Version v = Version.fromString(input, mode)

        then:
        v.qualifier == expectedQualifier

        where:
        input                  | expectedQualifier | mode
        '1.2.3'                | null              | Version.Mode.STRICT
        '1.2.3-rc1'            | 'rc1'             | Version.Mode.STRICT
        '1.2.3-SNAPSHOT'       | 'SNAPSHOT'        | Version.Mode.STRICT
        '1.2.3-SNAPSHOT-EXTRA' | 'SNAPSHOT-EXTRA'  | Version.Mode.RELAXED
    }

    private boolean assertVersionEquals(String stringVersion, int major, int minor, int revision, Version.Mode mode) {
        Version version = Version.fromString(stringVersion, mode);
        assert major == version.getMajor()
        assert minor == version.getMinor()
        assert revision == version.getRevision()
        true
    }

}
