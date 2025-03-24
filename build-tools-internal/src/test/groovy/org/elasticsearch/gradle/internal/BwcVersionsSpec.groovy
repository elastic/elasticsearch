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

import org.elasticsearch.gradle.Version
import org.elasticsearch.gradle.internal.BwcVersions.UnreleasedVersionInfo

class BwcVersionsSpec extends Specification {
    List<String> versionLines = []

    def "current version is next major"() {
        given:
        addVersion('7.17.10', '8.9.0')
        addVersion('8.14.0', '9.9.0')
        addVersion('8.14.1', '9.9.0')
        addVersion('8.14.2', '9.9.0')
        addVersion('8.15.0', '9.9.0')
        addVersion('8.15.1', '9.9.0')
        addVersion('8.15.2', '9.9.0')
        addVersion('8.16.0', '9.10.0')
        addVersion('8.16.1', '9.10.0')
        addVersion('8.17.0', '9.10.0')
        addVersion('9.0.0', '10.0.0')

        when:
        def bwc = new BwcVersions(versionLines, v('9.0.0'), ['main', '8.x', '8.16', '8.15', '7.17'])
        def unreleased = bwc.unreleased.collectEntries { [it, bwc.unreleasedInfo(it)] }

        then:
        unreleased == [
            (v('8.15.2')): new UnreleasedVersionInfo(v('8.15.2'), '8.15', ':distribution:bwc:bugfix2'),
            (v('8.16.1')): new UnreleasedVersionInfo(v('8.16.1'), '8.16', ':distribution:bwc:bugfix'),
            (v('8.17.0')): new UnreleasedVersionInfo(v('8.17.0'), '8.x', ':distribution:bwc:minor'),
            (v('9.0.0')): new UnreleasedVersionInfo(v('9.0.0'), 'main', ':distribution'),
        ]
        bwc.wireCompatible == [v('8.17.0'), v('9.0.0')]
        bwc.indexCompatible == [v('8.14.0'), v('8.14.1'), v('8.14.2'), v('8.15.0'), v('8.15.1'), v('8.15.2'), v('8.16.0'), v('8.16.1'), v('8.17.0'), v('9.0.0')]
    }

    def "current version is next major with staged minor"() {
        given:
        addVersion('7.17.10', '8.9.0')
        addVersion('8.14.0', '9.9.0')
        addVersion('8.14.1', '9.9.0')
        addVersion('8.14.2', '9.9.0')
        addVersion('8.15.0', '9.9.0')
        addVersion('8.15.1', '9.9.0')
        addVersion('8.15.2', '9.9.0')
        addVersion('8.16.0', '9.10.0')
        addVersion('8.16.1', '9.10.0')
        addVersion('8.17.0', '9.10.0')
        addVersion('8.18.0', '9.10.0')
        addVersion('9.0.0', '10.0.0')

        when:
        def bwc = new BwcVersions(versionLines, v('9.0.0'), ['main', '8.x', '8.17', '8.16', '8.15', '7.17'])
        def unreleased = bwc.unreleased.collectEntries { [it, bwc.unreleasedInfo(it)] }

        then:
        unreleased == [
            (v('8.15.2')): new UnreleasedVersionInfo(v('8.15.2'), '8.15', ':distribution:bwc:bugfix2'),
            (v('8.16.1')): new UnreleasedVersionInfo(v('8.16.1'), '8.16', ':distribution:bwc:bugfix'),
            (v('8.17.0')): new UnreleasedVersionInfo(v('8.17.0'), '8.17', ':distribution:bwc:staged'),
            (v('8.18.0')): new UnreleasedVersionInfo(v('8.18.0'), '8.x', ':distribution:bwc:minor'),
            (v('9.0.0')): new UnreleasedVersionInfo(v('9.0.0'), 'main', ':distribution'),
        ]
        bwc.wireCompatible == [v('8.18.0'), v('9.0.0')]
        bwc.indexCompatible == [v('8.14.0'), v('8.14.1'), v('8.14.2'), v('8.15.0'), v('8.15.1'), v('8.15.2'), v('8.16.0'), v('8.16.1'), v('8.17.0'), v('8.18.0'), v('9.0.0')]
    }

    def "current version is next major with two staged minors"() {
        given:
        addVersion('7.17.10', '8.9.0')
        addVersion('8.15.0', '9.9.0')
        addVersion('8.15.1', '9.9.0')
        addVersion('8.15.2', '9.9.0')
        addVersion('8.16.0', '9.10.0')
        addVersion('8.16.1', '9.10.0')
        addVersion('8.16.2', '9.10.0')
        addVersion('8.17.0', '9.10.0')
        addVersion('8.17.1', '9.10.0')
        addVersion('8.18.0', '9.10.0')
        addVersion('8.19.0', '9.10.0')
        addVersion('9.0.0', '10.0.0')
        addVersion('9.1.0', '10.1.0')

        when:
        def bwc = new BwcVersions(versionLines, v('9.1.0'), ['main', '9.0', '8.x', '8.18', '8.17', '8.16', '7.17'])
        def unreleased = bwc.unreleased.collectEntries { [it, bwc.unreleasedInfo(it)] }

        then:
        unreleased == [
            (v('8.16.2')): new UnreleasedVersionInfo(v('8.16.2'), '8.16', ':distribution:bwc:bugfix2'),
            (v('8.17.1')): new UnreleasedVersionInfo(v('8.17.1'), '8.17', ':distribution:bwc:bugfix'),
            (v('8.18.0')): new UnreleasedVersionInfo(v('8.18.0'), '8.18', ':distribution:bwc:staged2'),
            (v('8.19.0')): new UnreleasedVersionInfo(v('8.19.0'), '8.x', ':distribution:bwc:minor'),
            (v('9.0.0')): new UnreleasedVersionInfo(v('9.0.0'), '9.0', ':distribution:bwc:staged'),
            (v('9.1.0')): new UnreleasedVersionInfo(v('9.1.0'), 'main', ':distribution'),
        ]
        bwc.wireCompatible == [v('8.19.0'), v('9.0.0'), v('9.1.0')]
        bwc.indexCompatible == [v('8.15.0'), v('8.15.1'), v('8.15.2'), v('8.16.0'), v('8.16.1'), v('8.16.2'), v('8.17.0'), v('8.17.1'), v('8.18.0'), v('8.19.0'), v('9.0.0'), v('9.1.0')]
    }

    def "current version is first new minor in major series"() {
        given:
        addVersion('7.17.10', '8.9.0')
        addVersion('8.16.0', '9.10.0')
        addVersion('8.16.1', '9.10.0')
        addVersion('8.17.0', '9.10.0')
        addVersion('8.18.0', '9.10.0')
        addVersion('9.0.0', '10.0.0')
        addVersion('9.1.0', '10.0.0')

        when:
        def bwc = new BwcVersions(versionLines, v('9.1.0'), ['main', '9.0', '8.18'])
        def unreleased = bwc.unreleased.collectEntries { [it, bwc.unreleasedInfo(it)] }

        then:
        unreleased == [
            (v('8.18.0')): new UnreleasedVersionInfo(v('8.18.0'), '8.18', ':distribution:bwc:maintenance'),
            (v('9.0.0')): new UnreleasedVersionInfo(v('9.0.0'), '9.0', ':distribution:bwc:staged'),
            (v('9.1.0')): new UnreleasedVersionInfo(v('9.1.0'), 'main', ':distribution'),
        ]
        bwc.wireCompatible == [v('8.18.0'), v('9.0.0'), v('9.1.0')]
        bwc.indexCompatible == [v('8.16.0'), v('8.16.1'), v('8.17.0'), v('8.18.0'), v('9.0.0'), v('9.1.0')]
    }

    def "current version is new minor with single bugfix"() {
        given:
        addVersion('7.17.10', '8.9.0')
        addVersion('8.16.0', '9.10.0')
        addVersion('8.16.1', '9.10.0')
        addVersion('8.17.0', '9.10.0')
        addVersion('8.18.0', '9.10.0')
        addVersion('9.0.0', '10.0.0')
        addVersion('9.0.1', '10.0.0')
        addVersion('9.1.0', '10.0.0')

        when:
        def bwc = new BwcVersions(versionLines, v('9.1.0'), ['main', '9.0', '8.18'])
        def unreleased = bwc.unreleased.collectEntries { [it, bwc.unreleasedInfo(it)] }

        then:
        unreleased == [
            (v('8.18.0')): new UnreleasedVersionInfo(v('8.18.0'), '8.18', ':distribution:bwc:maintenance'),
            (v('9.0.1')): new UnreleasedVersionInfo(v('9.0.1'), '9.0', ':distribution:bwc:bugfix'),
            (v('9.1.0')): new UnreleasedVersionInfo(v('9.1.0'), 'main', ':distribution'),
        ]
        bwc.wireCompatible == [v('8.18.0'), v('9.0.0'), v('9.0.1'), v('9.1.0')]
        bwc.indexCompatible == [v('8.16.0'), v('8.16.1'), v('8.17.0'), v('8.18.0'), v('9.0.0'), v('9.0.1'), v('9.1.0')]
    }

    def "current version is new minor with single bugfix and staged minor"() {
        given:
        addVersion('7.17.10', '8.9.0')
        addVersion('8.16.0', '9.10.0')
        addVersion('8.16.1', '9.10.0')
        addVersion('8.17.0', '9.10.0')
        addVersion('8.18.0', '9.10.0')
        addVersion('9.0.0', '10.0.0')
        addVersion('9.0.1', '10.0.0')
        addVersion('9.1.0', '10.0.0')
        addVersion('9.2.0', '10.0.0')

        when:
        def bwc = new BwcVersions(versionLines, v('9.2.0'), ['main', '9.1', '9.0', '8.18'])
        def unreleased = bwc.unreleased.collectEntries { [it, bwc.unreleasedInfo(it)] }

        then:
        unreleased == [
            (v('8.18.0')): new UnreleasedVersionInfo(v('8.18.0'), '8.18', ':distribution:bwc:maintenance'),
            (v('9.0.1')): new UnreleasedVersionInfo(v('9.0.1'), '9.0', ':distribution:bwc:bugfix'),
            (v('9.1.0')): new UnreleasedVersionInfo(v('9.1.0'), '9.1', ':distribution:bwc:staged'),
            (v('9.2.0')): new UnreleasedVersionInfo(v('9.2.0'), 'main', ':distribution'),
        ]
        bwc.wireCompatible == [v('8.18.0'), v('9.0.0'), v('9.0.1'), v('9.1.0'), v('9.2.0')]
        bwc.indexCompatible == [v('8.16.0'), v('8.16.1'), v('8.17.0'), v('8.18.0'), v('9.0.0'), v('9.0.1'), v('9.1.0'), v('9.2.0')]
    }

    def "current version is next minor"() {
        given:
        addVersion('7.16.3', '8.9.0')
        addVersion('7.17.0', '8.9.0')
        addVersion('7.17.1', '8.9.0')
        addVersion('8.14.0', '9.9.0')
        addVersion('8.14.1', '9.9.0')
        addVersion('8.14.2', '9.9.0')
        addVersion('8.15.0', '9.9.0')
        addVersion('8.15.1', '9.9.0')
        addVersion('8.15.2', '9.9.0')
        addVersion('8.16.0', '9.10.0')
        addVersion('8.16.1', '9.10.0')
        addVersion('8.17.0', '9.10.0')
        addVersion('8.17.1', '9.10.0')
        addVersion('8.18.0', '9.10.0')

        when:
        def bwc = new BwcVersions(versionLines, v('8.18.0'), ['main', '8.x', '8.17', '8.16', '7.17'])
        def unreleased = bwc.unreleased.collectEntries { [it, bwc.unreleasedInfo(it)] }

        then:
        unreleased == [
            (v('7.17.1')): new UnreleasedVersionInfo(v('7.17.1'), '7.17', ':distribution:bwc:maintenance'),
            (v('8.16.1')): new UnreleasedVersionInfo(v('8.16.1'), '8.16', ':distribution:bwc:bugfix2'),
            (v('8.17.1')): new UnreleasedVersionInfo(v('8.17.1'), '8.17', ':distribution:bwc:bugfix'),
            (v('8.18.0')): new UnreleasedVersionInfo(v('8.18.0'), '8.x', ':distribution'),
        ]
        bwc.wireCompatible == [v('7.17.0'), v('7.17.1'), v('8.14.0'), v('8.14.1'), v('8.14.2'), v('8.15.0'), v('8.15.1'), v('8.15.2'), v('8.16.0'), v('8.16.1'), v('8.17.0'), v('8.17.1'), v('8.18.0')]
        bwc.indexCompatible == [v('7.16.3'), v('7.17.0'), v('7.17.1'), v('8.14.0'), v('8.14.1'), v('8.14.2'), v('8.15.0'), v('8.15.1'), v('8.15.2'), v('8.16.0'), v('8.16.1'), v('8.17.0'), v('8.17.1'), v('8.18.0')]
    }

    def "current version is new minor with staged minor"() {
        given:
        addVersion('7.16.3', '8.9.0')
        addVersion('7.17.0', '8.9.0')
        addVersion('7.17.1', '8.9.0')
        addVersion('8.14.0', '9.9.0')
        addVersion('8.14.1', '9.9.0')
        addVersion('8.14.2', '9.9.0')
        addVersion('8.15.0', '9.9.0')
        addVersion('8.15.1', '9.9.0')
        addVersion('8.15.2', '9.9.0')
        addVersion('8.16.0', '9.10.0')
        addVersion('8.16.1', '9.10.0')
        addVersion('8.17.0', '9.10.0')
        addVersion('8.18.0', '9.10.0')

        when:
        def bwc = new BwcVersions(versionLines, v('8.18.0'), ['main', '8.x', '8.17', '8.16', '8.15', '7.17'])
        def unreleased = bwc.unreleased.collectEntries { [it, bwc.unreleasedInfo(it)] }

        then:
        unreleased == [
            (v('7.17.1')): new UnreleasedVersionInfo(v('7.17.1'), '7.17', ':distribution:bwc:maintenance'),
            (v('8.15.2')): new UnreleasedVersionInfo(v('8.15.2'), '8.15', ':distribution:bwc:bugfix2'),
            (v('8.16.1')): new UnreleasedVersionInfo(v('8.16.1'), '8.16', ':distribution:bwc:bugfix'),
            (v('8.17.0')): new UnreleasedVersionInfo(v('8.17.0'), '8.17', ':distribution:bwc:staged'),
            (v('8.18.0')): new UnreleasedVersionInfo(v('8.18.0'), '8.x', ':distribution'),
        ]
        bwc.wireCompatible == [v('7.17.0'), v('7.17.1'), v('8.14.0'), v('8.14.1'), v('8.14.2'), v('8.15.0'), v('8.15.1'), v('8.15.2'), v('8.16.0'), v('8.16.1'), v('8.17.0'), v('8.18.0')]
        bwc.indexCompatible == [v('7.16.3'), v('7.17.0'), v('7.17.1'), v('8.14.0'), v('8.14.1'), v('8.14.2'), v('8.15.0'), v('8.15.1'), v('8.15.2'), v('8.16.0'), v('8.16.1'), v('8.17.0'), v('8.18.0')]
    }

    def "current version is first bugfix"() {
        given:
        addVersion('7.16.3', '8.9.0')
        addVersion('7.17.0', '8.9.0')
        addVersion('7.17.1', '8.9.0')
        addVersion('8.14.0', '9.9.0')
        addVersion('8.14.1', '9.9.0')
        addVersion('8.14.2', '9.9.0')
        addVersion('8.15.0', '9.9.0')
        addVersion('8.15.1', '9.9.0')
        addVersion('8.15.2', '9.9.0')
        addVersion('8.16.0', '9.10.0')
        addVersion('8.16.1', '9.10.0')

        when:
        def bwc = new BwcVersions(versionLines, v('8.16.1'), ['main', '8.x', '8.17', '8.16', '8.15', '7.17'])
        def unreleased = bwc.unreleased.collectEntries { [it, bwc.unreleasedInfo(it)] }

        then:
        unreleased == [
            (v('7.17.1')): new UnreleasedVersionInfo(v('7.17.1'), '7.17', ':distribution:bwc:maintenance'),
            (v('8.15.2')): new UnreleasedVersionInfo(v('8.15.2'), '8.15', ':distribution:bwc:bugfix'),
            (v('8.16.1')): new UnreleasedVersionInfo(v('8.16.1'), '8.16', ':distribution'),
        ]
        bwc.wireCompatible == [v('7.17.0'), v('7.17.1'), v('8.14.0'), v('8.14.1'), v('8.14.2'), v('8.15.0'), v('8.15.1'), v('8.15.2'), v('8.16.0'), v('8.16.1')]
        bwc.indexCompatible == [v('7.16.3'), v('7.17.0'), v('7.17.1'), v('8.14.0'), v('8.14.1'), v('8.14.2'), v('8.15.0'), v('8.15.1'), v('8.15.2'), v('8.16.0'), v('8.16.1')]
    }

    def "current version is second bugfix"() {
        given:
        addVersion('7.16.3', '8.9.0')
        addVersion('7.17.0', '8.9.0')
        addVersion('7.17.1', '8.9.0')
        addVersion('8.14.0', '9.9.0')
        addVersion('8.14.1', '9.9.0')
        addVersion('8.14.2', '9.9.0')
        addVersion('8.15.0', '9.9.0')
        addVersion('8.15.1', '9.9.0')
        addVersion('8.15.2', '9.9.0')

        when:
        def bwc = new BwcVersions(versionLines, v('8.15.2'), ['main', '8.x', '8.17', '8.16', '8.15', '7.17'])
        def unreleased = bwc.unreleased.collectEntries { [it, bwc.unreleasedInfo(it)] }

        then:
        unreleased == [
            (v('7.17.1')): new UnreleasedVersionInfo(v('7.17.1'), '7.17', ':distribution:bwc:maintenance'),
            (v('8.15.2')): new UnreleasedVersionInfo(v('8.15.2'), '8.15', ':distribution'),
        ]
        bwc.wireCompatible == [v('7.17.0'), v('7.17.1'), v('8.14.0'), v('8.14.1'), v('8.14.2'), v('8.15.0'), v('8.15.1'), v('8.15.2')]
        bwc.indexCompatible == [v('7.16.3'), v('7.17.0'), v('7.17.1'), v('8.14.0'), v('8.14.1'), v('8.14.2'), v('8.15.0'), v('8.15.1'), v('8.15.2')]
    }

    private void addVersion(String elasticsearch, String lucene) {
        def es = Version.fromString(elasticsearch)
        def l = Version.fromString(lucene)
        versionLines << "    public static final Version V_${es.major}_${es.minor}_${es.revision} = new Version(0000000, org.apache.lucene.util.Version.LUCENE_${l.major}_${l.minor}_${l.revision});".toString()
    }

    private Version v(String version) {
        return Version.fromString(version)
    }

}
