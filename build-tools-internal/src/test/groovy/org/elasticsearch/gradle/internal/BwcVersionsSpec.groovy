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
import org.elasticsearch.gradle.internal.info.DevelopmentBranch

class BwcVersionsSpec extends Specification {
    List<Version> versions = []

    def "current version is next major"() {
        given:
        addVersion('7.17.10')
        addVersion('8.14.0')
        addVersion('8.14.1')
        addVersion('8.14.2')
        addVersion('8.15.0')
        addVersion('8.15.1')
        addVersion('8.15.2')
        addVersion('8.16.0')
        addVersion('8.16.1')
        addVersion('8.17.0')
        addVersion('9.0.0')

        when:
        def bwc = new BwcVersions(v('9.0.0'), versions, [
            branch('main', '9.0.0'),
            branch('8.x', '8.17.0'),
            branch('8.16', '8.16.1'),
            branch('8.15', '8.15.2'),
            branch('7.17', '7.17.10')
        ])
        def unreleased = bwc.unreleased.collectEntries { [it, bwc.unreleasedInfo(it)] }

        then:
        unreleased == [
            (v('8.15.2')): new UnreleasedVersionInfo(v('8.15.2'), '8.15', ':distribution:bwc:major3'),
            (v('8.16.1')): new UnreleasedVersionInfo(v('8.16.1'), '8.16', ':distribution:bwc:major2'),
            (v('8.17.0')): new UnreleasedVersionInfo(v('8.17.0'), '8.x', ':distribution:bwc:major1'),
            (v('9.0.0')): new UnreleasedVersionInfo(v('9.0.0'), 'main', ':distribution'),
        ]
        bwc.wireCompatible == [v('8.17.0'), v('9.0.0')]
        bwc.indexCompatible == [v('8.14.0'), v('8.14.1'), v('8.14.2'), v('8.15.0'), v('8.15.1'), v('8.15.2'), v('8.16.0'), v('8.16.1'), v('8.17.0'), v('9.0.0')]
    }

    def "current version is next major with staged minor"() {
        given:
        addVersion('7.17.10')
        addVersion('8.14.0')
        addVersion('8.14.1')
        addVersion('8.14.2')
        addVersion('8.15.0')
        addVersion('8.15.1')
        addVersion('8.15.2')
        addVersion('8.16.0')
        addVersion('8.16.1')
        addVersion('8.17.0')
        addVersion('8.18.0')
        addVersion('9.0.0')

        when:
        def bwc = new BwcVersions(v('9.0.0'), versions, [
            branch('main', '9.0.0'),
            branch('8.x', '8.18.0'),
            branch('8.17', '8.17.0'),
            branch('8.16', '8.16.1'),
            branch('8.15', '8.15.2'),
            branch('7.17', '7.17.10'),
        ])
        def unreleased = bwc.unreleased.collectEntries { [it, bwc.unreleasedInfo(it)] }

        then:
        unreleased == [
            (v('8.15.2')): new UnreleasedVersionInfo(v('8.15.2'), '8.15', ':distribution:bwc:major4'),
            (v('8.16.1')): new UnreleasedVersionInfo(v('8.16.1'), '8.16', ':distribution:bwc:major3'),
            (v('8.17.0')): new UnreleasedVersionInfo(v('8.17.0'), '8.17', ':distribution:bwc:major2'),
            (v('8.18.0')): new UnreleasedVersionInfo(v('8.18.0'), '8.x', ':distribution:bwc:major1'),
            (v('9.0.0')): new UnreleasedVersionInfo(v('9.0.0'), 'main', ':distribution'),
        ]
        bwc.wireCompatible == [v('8.18.0'), v('9.0.0')]
        bwc.indexCompatible == [v('8.14.0'), v('8.14.1'), v('8.14.2'), v('8.15.0'), v('8.15.1'), v('8.15.2'), v('8.16.0'), v('8.16.1'), v('8.17.0'), v('8.18.0'), v('9.0.0')]
    }

    def "current version is next major with two staged minors"() {
        given:
        addVersion('7.17.10')
        addVersion('8.15.0')
        addVersion('8.15.1')
        addVersion('8.15.2')
        addVersion('8.16.0')
        addVersion('8.16.1')
        addVersion('8.16.2')
        addVersion('8.17.0')
        addVersion('8.17.1')
        addVersion('8.18.0')
        addVersion('8.19.0')
        addVersion('9.0.0')
        addVersion('9.1.0')

        when:
        def bwc = new BwcVersions(v('9.1.0'), versions, [
            branch('main', '9.1.0'),
            branch('9.0', '9.0.0'),
            branch('8.x', '8.19.0'),
            branch('8.18', '8.18.0'),
            branch('8.17', '8.17.1'),
            branch('8.16', '8.16.2'),
            branch('7.17', '7.17.10')
        ])
        def unreleased = bwc.unreleased.collectEntries { [it, bwc.unreleasedInfo(it)] }

        then:
        unreleased == [
            (v('8.16.2')): new UnreleasedVersionInfo(v('8.16.2'), '8.16', ':distribution:bwc:major4'),
            (v('8.17.1')): new UnreleasedVersionInfo(v('8.17.1'), '8.17', ':distribution:bwc:major3'),
            (v('8.18.0')): new UnreleasedVersionInfo(v('8.18.0'), '8.18', ':distribution:bwc:major2'),
            (v('8.19.0')): new UnreleasedVersionInfo(v('8.19.0'), '8.x', ':distribution:bwc:major1'),
            (v('9.0.0')): new UnreleasedVersionInfo(v('9.0.0'), '9.0', ':distribution:bwc:minor1'),
            (v('9.1.0')): new UnreleasedVersionInfo(v('9.1.0'), 'main', ':distribution'),
        ]
        bwc.wireCompatible == [v('8.19.0'), v('9.0.0'), v('9.1.0')]
        bwc.indexCompatible == [v('8.15.0'), v('8.15.1'), v('8.15.2'), v('8.16.0'), v('8.16.1'), v('8.16.2'), v('8.17.0'), v('8.17.1'), v('8.18.0'), v('8.19.0'), v('9.0.0'), v('9.1.0')]
    }

    def "current version is first new minor in major series"() {
        given:
        addVersion('7.17.10')
        addVersion('8.16.0')
        addVersion('8.16.1')
        addVersion('8.17.0')
        addVersion('8.18.0')
        addVersion('9.0.0')
        addVersion('9.1.0')

        when:
        def bwc = new BwcVersions(v('9.1.0'), versions, [
            branch('main','9.1.0'),
            branch('9.0', '9.0.0'),
            branch('8.18', '8.18.0')
        ])
        def unreleased = bwc.unreleased.collectEntries { [it, bwc.unreleasedInfo(it)] }

        then:
        unreleased == [
            (v('8.18.0')): new UnreleasedVersionInfo(v('8.18.0'), '8.18', ':distribution:bwc:major1'),
            (v('9.0.0')): new UnreleasedVersionInfo(v('9.0.0'), '9.0', ':distribution:bwc:minor1'),
            (v('9.1.0')): new UnreleasedVersionInfo(v('9.1.0'), 'main', ':distribution'),
        ]
        bwc.wireCompatible == [v('8.18.0'), v('9.0.0'), v('9.1.0')]
        bwc.indexCompatible == [v('8.16.0'), v('8.16.1'), v('8.17.0'), v('8.18.0'), v('9.0.0'), v('9.1.0')]
    }

    def "current version is new minor with single bugfix"() {
        given:
        addVersion('7.17.10')
        addVersion('8.16.0')
        addVersion('8.16.1')
        addVersion('8.17.0')
        addVersion('8.18.0')
        addVersion('9.0.0')
        addVersion('9.0.1')
        addVersion('9.1.0')

        when:
        def bwc = new BwcVersions(v('9.1.0'), versions, [
            branch('main','9.1.0'),
            branch('9.0','9.0.1'),
            branch('8.18','8.18.0')
        ])
        def unreleased = bwc.unreleased.collectEntries { [it, bwc.unreleasedInfo(it)] }

        then:
        unreleased == [
            (v('8.18.0')): new UnreleasedVersionInfo(v('8.18.0'), '8.18', ':distribution:bwc:major1'),
            (v('9.0.1')): new UnreleasedVersionInfo(v('9.0.1'), '9.0', ':distribution:bwc:minor1'),
            (v('9.1.0')): new UnreleasedVersionInfo(v('9.1.0'), 'main', ':distribution'),
        ]
        bwc.wireCompatible == [v('8.18.0'), v('9.0.0'), v('9.0.1'), v('9.1.0')]
        bwc.indexCompatible == [v('8.16.0'), v('8.16.1'), v('8.17.0'), v('8.18.0'), v('9.0.0'), v('9.0.1'), v('9.1.0')]
    }

    def "current version is new minor with single bugfix and staged minor"() {
        given:
        addVersion('7.17.10')
        addVersion('8.16.0')
        addVersion('8.16.1')
        addVersion('8.17.0')
        addVersion('8.18.0')
        addVersion('9.0.0')
        addVersion('9.0.1')
        addVersion('9.1.0')
        addVersion('9.2.0')

        when:
        def bwc = new BwcVersions(v('9.2.0'), versions, [
            branch('main','9.2.0'),
            branch('9.1','9.1.0'),
            branch('9.0','9.0.1'),
            branch('8.18', '8.18.0')
        ])
        def unreleased = bwc.unreleased.collectEntries { [it, bwc.unreleasedInfo(it)] }

        then:
        unreleased == [
            (v('8.18.0')): new UnreleasedVersionInfo(v('8.18.0'), '8.18', ':distribution:bwc:major1'),
            (v('9.0.1')): new UnreleasedVersionInfo(v('9.0.1'), '9.0', ':distribution:bwc:minor2'),
            (v('9.1.0')): new UnreleasedVersionInfo(v('9.1.0'), '9.1', ':distribution:bwc:minor1'),
            (v('9.2.0')): new UnreleasedVersionInfo(v('9.2.0'), 'main', ':distribution'),
        ]
        bwc.wireCompatible == [v('8.18.0'), v('9.0.0'), v('9.0.1'), v('9.1.0'), v('9.2.0')]
        bwc.indexCompatible == [v('8.16.0'), v('8.16.1'), v('8.17.0'), v('8.18.0'), v('9.0.0'), v('9.0.1'), v('9.1.0'), v('9.2.0')]
    }

    def "current version is next minor"() {
        given:
        addVersion('7.16.3')
        addVersion('7.17.0')
        addVersion('7.17.1')
        addVersion('8.14.0')
        addVersion('8.14.1')
        addVersion('8.14.2')
        addVersion('8.15.0')
        addVersion('8.15.1')
        addVersion('8.15.2')
        addVersion('8.16.0')
        addVersion('8.16.1')
        addVersion('8.17.0')
        addVersion('8.17.1')
        addVersion('8.18.0')

        when:
        def bwc = new BwcVersions(v('8.18.0'), versions, [
            branch('main', '9.1.0'),
            branch('9.0', '9.0.1'),
            branch('8.x', '8.18.0'),
            branch('8.17', '8.17.1'),
            branch('8.16', '8.16.1'),
            branch('7.17', '7.17.1'),
        ])
        def unreleased = bwc.unreleased.collectEntries { [it, bwc.unreleasedInfo(it)] }

        then:
        unreleased == [
            (v('7.17.1')): new UnreleasedVersionInfo(v('7.17.1'), '7.17', ':distribution:bwc:major1'),
            (v('8.16.1')): new UnreleasedVersionInfo(v('8.16.1'), '8.16', ':distribution:bwc:minor2'),
            (v('8.17.1')): new UnreleasedVersionInfo(v('8.17.1'), '8.17', ':distribution:bwc:minor1'),
            (v('8.18.0')): new UnreleasedVersionInfo(v('8.18.0'), '8.x', ':distribution'),
        ]
        bwc.wireCompatible == [v('7.17.0'), v('7.17.1'), v('8.14.0'), v('8.14.1'), v('8.14.2'), v('8.15.0'), v('8.15.1'), v('8.15.2'), v('8.16.0'), v('8.16.1'), v('8.17.0'), v('8.17.1'), v('8.18.0')]
        bwc.indexCompatible == [v('7.16.3'), v('7.17.0'), v('7.17.1'), v('8.14.0'), v('8.14.1'), v('8.14.2'), v('8.15.0'), v('8.15.1'), v('8.15.2'), v('8.16.0'), v('8.16.1'), v('8.17.0'), v('8.17.1'), v('8.18.0')]
    }

    def "current version is new minor with staged minor"() {
        given:
        addVersion('7.16.3')
        addVersion('7.17.0')
        addVersion('7.17.1')
        addVersion('8.14.0')
        addVersion('8.14.1')
        addVersion('8.14.2')
        addVersion('8.15.0')
        addVersion('8.15.1')
        addVersion('8.15.2')
        addVersion('8.16.0')
        addVersion('8.16.1')
        addVersion('8.17.0')
        addVersion('8.18.0')

        when:
        def bwc = new BwcVersions(v('8.18.0'), versions, [
            branch('main', '9.0.0'),
            branch('8.x', '8.18.0'),
            branch('8.17', '8.17.0'),
            branch('8.16', '8.16.1'),
            branch('8.15', '8.15.2'),
            branch('7.17', '7.17.1'),
        ])
        def unreleased = bwc.unreleased.collectEntries { [it, bwc.unreleasedInfo(it)] }

        then:
        unreleased == [
            (v('7.17.1')): new UnreleasedVersionInfo(v('7.17.1'), '7.17', ':distribution:bwc:major1'),
            (v('8.15.2')): new UnreleasedVersionInfo(v('8.15.2'), '8.15', ':distribution:bwc:minor3'),
            (v('8.16.1')): new UnreleasedVersionInfo(v('8.16.1'), '8.16', ':distribution:bwc:minor2'),
            (v('8.17.0')): new UnreleasedVersionInfo(v('8.17.0'), '8.17', ':distribution:bwc:minor1'),
            (v('8.18.0')): new UnreleasedVersionInfo(v('8.18.0'), '8.x', ':distribution'),
        ]
        bwc.wireCompatible == [v('7.17.0'), v('7.17.1'), v('8.14.0'), v('8.14.1'), v('8.14.2'), v('8.15.0'), v('8.15.1'), v('8.15.2'), v('8.16.0'), v('8.16.1'), v('8.17.0'), v('8.18.0')]
        bwc.indexCompatible == [v('7.16.3'), v('7.17.0'), v('7.17.1'), v('8.14.0'), v('8.14.1'), v('8.14.2'), v('8.15.0'), v('8.15.1'), v('8.15.2'), v('8.16.0'), v('8.16.1'), v('8.17.0'), v('8.18.0')]
    }

    def "current version is first bugfix"() {
        given:
        addVersion('7.16.3')
        addVersion('7.17.0')
        addVersion('7.17.1')
        addVersion('8.14.0')
        addVersion('8.14.1')
        addVersion('8.14.2')
        addVersion('8.15.0')
        addVersion('8.15.1')
        addVersion('8.15.2')
        addVersion('8.16.0')
        addVersion('8.16.1')

        when:
        def bwc = new BwcVersions(v('8.16.1'), versions, [
            branch('main','9.0.1'),
            branch('8.x','8.18.0'),
            branch('8.17','8.17.0'),
            branch('8.16','8.16.1'),
            branch('8.15','8.15.2'),
            branch('7.17','7.17.1'),
        ])
        def unreleased = bwc.unreleased.collectEntries { [it, bwc.unreleasedInfo(it)] }

        then:
        unreleased == [
            (v('7.17.1')): new UnreleasedVersionInfo(v('7.17.1'), '7.17', ':distribution:bwc:major1'),
            (v('8.15.2')): new UnreleasedVersionInfo(v('8.15.2'), '8.15', ':distribution:bwc:minor1'),
            (v('8.16.1')): new UnreleasedVersionInfo(v('8.16.1'), '8.16', ':distribution'),
        ]
        bwc.wireCompatible == [v('7.17.0'), v('7.17.1'), v('8.14.0'), v('8.14.1'), v('8.14.2'), v('8.15.0'), v('8.15.1'), v('8.15.2'), v('8.16.0'), v('8.16.1')]
        bwc.indexCompatible == [v('7.16.3'), v('7.17.0'), v('7.17.1'), v('8.14.0'), v('8.14.1'), v('8.14.2'), v('8.15.0'), v('8.15.1'), v('8.15.2'), v('8.16.0'), v('8.16.1')]
    }

    def "current version is second bugfix"() {
        given:
        addVersion('7.16.3')
        addVersion('7.17.0')
        addVersion('7.17.1')
        addVersion('8.14.0')
        addVersion('8.14.1')
        addVersion('8.14.2')
        addVersion('8.15.0')
        addVersion('8.15.1')
        addVersion('8.15.2')

        when:
        def bwc = new BwcVersions(v('8.15.2'), versions, [
            branch('main', '9.0.1'),
            branch('8.x', '8.18.1'),
            branch('8.17', '8.17.2'),
            branch('8.16', '8.16.10'),
            branch('8.15', '8.15.2'),
            branch('7.17', '7.17.1'),
        ])
        def unreleased = bwc.unreleased.collectEntries { [it, bwc.unreleasedInfo(it)] }

        then:
        unreleased == [
            (v('7.17.1')): new UnreleasedVersionInfo(v('7.17.1'), '7.17', ':distribution:bwc:major1'),
            (v('8.15.2')): new UnreleasedVersionInfo(v('8.15.2'), '8.15', ':distribution'),
        ]
        bwc.wireCompatible == [v('7.17.0'), v('7.17.1'), v('8.14.0'), v('8.14.1'), v('8.14.2'), v('8.15.0'), v('8.15.1'), v('8.15.2')]
        bwc.indexCompatible == [v('7.16.3'), v('7.17.0'), v('7.17.1'), v('8.14.0'), v('8.14.1'), v('8.14.2'), v('8.15.0'), v('8.15.1'), v('8.15.2')]
    }

    private void addVersion(String elasticsearch) {
        versions.add(Version.fromString(elasticsearch))
    }

    private Version v(String version) {
        return Version.fromString(version)
    }

    private DevelopmentBranch branch(String name, String version) {
        return new DevelopmentBranch(name, v(version))
    }

}
