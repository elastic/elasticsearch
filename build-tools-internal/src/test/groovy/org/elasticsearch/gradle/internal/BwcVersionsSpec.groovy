/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal

import spock.lang.Specification

import org.elasticsearch.gradle.Architecture
import org.elasticsearch.gradle.ElasticsearchDistribution
import org.elasticsearch.gradle.Version
import org.elasticsearch.gradle.internal.BwcVersions.UnreleasedVersionInfo


class BwcVersionsSpec extends Specification {
    List<String> versionLines = []

    def "current version is next major with last minor staged"() {
        given:
        addVersion('7.14.0', '8.9.0')
        addVersion('7.14.1', '8.9.0')
        addVersion('7.14.2', '8.9.0')
        addVersion('7.15.0', '8.9.0')
        addVersion('7.15.1', '8.9.0')
        addVersion('7.15.2', '8.9.0')
        addVersion('7.16.0', '8.10.0')
        addVersion('7.16.1', '8.10.0')
        addVersion('7.16.2', '8.10.0')
        addVersion('7.17.0', '8.10.0')
        addVersion('8.0.0', '9.0.0')
        addVersion('8.1.0', '9.0.0')

        when:
        def bwc = new BwcVersions(versionLines, v('8.1.0'))
        def unreleased = bwc.unreleased.collectEntries { [it, bwc.unreleasedInfo(it)] }

        then:
        unreleased == [
            (v('7.16.2')): new UnreleasedVersionInfo(v('7.16.2'), '7.16', ':distribution:bwc:bugfix'),
            (v('7.17.0')): new UnreleasedVersionInfo(v('7.17.0'), '7.17', ':distribution:bwc:staged'),
            (v('8.0.0')): new UnreleasedVersionInfo(v('8.0.0'), '8.0', ':distribution:bwc:minor'),
            (v('8.1.0')): new UnreleasedVersionInfo(v('8.1.0'), 'main', ':distribution')
        ]
        bwc.wireCompatible == [v('7.17.0'), v('8.0.0'), v('8.1.0')]
        bwc.indexCompatible == osFiltered([v('7.14.0'), v('7.14.1'), v('7.14.2'), v('7.15.0'), v('7.15.1'), v('7.15.2'), v('7.16.0'), v('7.16.1'), v('7.16.2'), v('7.17.0'), v('8.0.0'), v('8.1.0')])
    }

    def "current version is next minor with next major and last minor both staged"() {
        given:
        addVersion('7.14.0', '8.9.0')
        addVersion('7.14.1', '8.9.0')
        addVersion('7.14.2', '8.9.0')
        addVersion('7.15.0', '8.9.0')
        addVersion('7.15.1', '8.9.0')
        addVersion('7.15.2', '8.9.0')
        addVersion('7.16.0', '8.10.0')
        addVersion('7.16.1', '8.10.0')
        addVersion('7.17.0', '8.10.0')
        addVersion('8.0.0', '9.0.0')
        addVersion('8.1.0', '9.1.0')

        when:
        def bwc = new BwcVersions(versionLines, v('8.1.0'))
        def unreleased = bwc.unreleased.collectEntries { [it, bwc.unreleasedInfo(it)] }

        then:
        unreleased == [
            (v('7.16.1')): new UnreleasedVersionInfo(v('7.16.1'), '7.16', ':distribution:bwc:bugfix'),
            (v('7.17.0')): new UnreleasedVersionInfo(v('7.17.0'), '7.17', ':distribution:bwc:staged'),
            (v('8.0.0')): new UnreleasedVersionInfo(v('8.0.0'), '8.0', ':distribution:bwc:minor'),
            (v('8.1.0')): new UnreleasedVersionInfo(v('8.1.0'), 'main', ':distribution')
        ]
        bwc.wireCompatible == [v('7.17.0'), v('8.0.0'), v('8.1.0')]
        bwc.indexCompatible == osFiltered([v('7.14.0'), v('7.14.1'), v('7.14.2'), v('7.15.0'), v('7.15.1'), v('7.15.2'), v('7.16.0'), v('7.16.1'), v('7.17.0'), v('8.0.0'), v('8.1.0')])
    }

    def "current is next minor with upcoming minor staged"() {
        given:
        addVersion('7.14.0', '8.9.0')
        addVersion('7.14.1', '8.9.0')
        addVersion('7.14.2', '8.9.0')
        addVersion('7.15.0', '8.9.0')
        addVersion('7.15.1', '8.9.0')
        addVersion('7.15.2', '8.9.0')
        addVersion('7.16.0', '8.10.0')
        addVersion('7.16.1', '8.10.0')
        addVersion('7.17.0', '8.10.0')
        addVersion('7.17.1', '8.10.0')
        addVersion('8.0.0', '9.0.0')
        addVersion('8.1.0', '9.1.0')

        when:
        def bwc = new BwcVersions(versionLines, v('8.1.0'))
        def unreleased = bwc.unreleased.collectEntries { [it, bwc.unreleasedInfo(it)] }

        then:
        unreleased == [
            (v('7.17.1')): new UnreleasedVersionInfo(v('7.17.1'), '7.17', ':distribution:bwc:bugfix'),
            (v('8.0.0')): new UnreleasedVersionInfo(v('8.0.0'), '8.0', ':distribution:bwc:staged'),
            (v('8.1.0')): new UnreleasedVersionInfo(v('8.1.0'), 'main', ':distribution')
        ]
        bwc.wireCompatible == [v('7.17.0'), v('7.17.1'), v('8.0.0'), v('8.1.0')]
        bwc.indexCompatible == osFiltered([v('7.14.0'), v('7.14.1'), v('7.14.2'), v('7.15.0'), v('7.15.1'), v('7.15.2'), v('7.16.0'), v('7.16.1'), v('7.17.0'), v('7.17.1'), v('8.0.0'), v('8.1.0')])
    }

    def "current version is staged major"() {
        given:
        addVersion('7.14.0', '8.9.0')
        addVersion('7.14.1', '8.9.0')
        addVersion('7.14.2', '8.9.0')
        addVersion('7.15.0', '8.9.0')
        addVersion('7.15.1', '8.9.0')
        addVersion('7.15.2', '8.9.0')
        addVersion('7.16.0', '8.10.0')
        addVersion('7.16.1', '8.10.0')
        addVersion('7.17.0', '8.10.0')
        addVersion('7.17.1', '8.10.0')
        addVersion('8.0.0', '9.0.0')

        when:
        def bwc = new BwcVersions(versionLines, v('8.0.0'))
        def unreleased = bwc.unreleased.collectEntries { [it, bwc.unreleasedInfo(it)] }

        then:
        unreleased == [
            (v('7.17.1')): new UnreleasedVersionInfo(v('7.17.1'), '7.17', ':distribution:bwc:bugfix'),
            (v('8.0.0')): new UnreleasedVersionInfo(v('8.0.0'), 'main', ':distribution'),
        ]
        bwc.wireCompatible == [v('7.17.0'), v('7.17.1'), v('8.0.0')]
        bwc.indexCompatible == osFiltered([v('7.14.0'), v('7.14.1'), v('7.14.2'), v('7.15.0'), v('7.15.1'), v('7.15.2'), v('7.16.0'), v('7.16.1'), v('7.17.0'), v('7.17.1'), v('8.0.0')])
    }

    def "current version is next bugfix"() {
        given:
        addVersion('7.14.0', '8.9.0')
        addVersion('7.14.1', '8.9.0')
        addVersion('7.14.2', '8.9.0')
        addVersion('7.15.0', '8.9.0')
        addVersion('7.15.1', '8.9.0')
        addVersion('7.15.2', '8.9.0')
        addVersion('7.16.0', '8.10.0')
        addVersion('7.16.1', '8.10.0')
        addVersion('7.17.0', '8.10.0')
        addVersion('7.17.1', '8.10.0')
        addVersion('8.0.0', '9.0.0')
        addVersion('8.0.1', '9.0.0')

        when:
        def bwc = new BwcVersions(versionLines, v('8.0.1'))
        def unreleased = bwc.unreleased.collectEntries { [it, bwc.unreleasedInfo(it)] }

        then:
        unreleased == [
            (v('7.17.1')): new UnreleasedVersionInfo(v('7.17.1'), '7.17', ':distribution:bwc:maintenance'),
            (v('8.0.1')): new UnreleasedVersionInfo(v('8.0.1'), 'main', ':distribution'),
        ]
        bwc.wireCompatible == [v('7.17.0'), v('7.17.1'), v('8.0.0'), v('8.0.1')]
        bwc.indexCompatible == osFiltered([v('7.14.0'), v('7.14.1'), v('7.14.2'), v('7.15.0'), v('7.15.1'), v('7.15.2'), v('7.16.0'), v('7.16.1'), v('7.17.0'), v('7.17.1'), v('8.0.0'), v('8.0.1')])
    }

    def "current version is next minor with no staged releases"() {
        given:
        addVersion('7.14.0', '8.9.0')
        addVersion('7.14.1', '8.9.0')
        addVersion('7.14.2', '8.9.0')
        addVersion('7.15.0', '8.9.0')
        addVersion('7.15.1', '8.9.0')
        addVersion('7.15.2', '8.9.0')
        addVersion('7.16.0', '8.10.0')
        addVersion('7.16.1', '8.10.0')
        addVersion('7.17.0', '8.10.0')
        addVersion('7.17.1', '8.10.0')
        addVersion('8.0.0', '9.0.0')
        addVersion('8.0.1', '9.0.0')
        addVersion('8.1.0', '9.1.0')

        when:
        def bwc = new BwcVersions(versionLines, v('8.1.0'))
        def unreleased = bwc.unreleased.collectEntries { [it, bwc.unreleasedInfo(it)] }

        then:
        unreleased == [
            (v('7.17.1')): new UnreleasedVersionInfo(v('7.17.1'), '7.17', ':distribution:bwc:maintenance'),
            (v('8.0.1')): new UnreleasedVersionInfo(v('8.0.1'), '8.0', ':distribution:bwc:bugfix'),
            (v('8.1.0')): new UnreleasedVersionInfo(v('8.1.0'), 'main', ':distribution')
        ]
        bwc.wireCompatible == [v('7.17.0'), v('7.17.1'), v('8.0.0'), v('8.0.1'), v('8.1.0')]
        bwc.indexCompatible == osFiltered([v('7.14.0'), v('7.14.1'), v('7.14.2'), v('7.15.0'), v('7.15.1'), v('7.15.2'), v('7.16.0'), v('7.16.1'), v('7.17.0'), v('7.17.1'), v('8.0.0'), v('8.0.1'), v('8.1.0')])
    }

    private void addVersion(String elasticsearch, String lucene) {
        def es = Version.fromString(elasticsearch)
        def l = Version.fromString(lucene)
        versionLines << "    public static final Version V_${es.major}_${es.minor}_${es.revision} = new Version(0000000, org.apache.lucene.util.Version.LUCENE_${l.major}_${l.minor}_${l.revision});".toString()
    }

    private Version v(String version) {
        return Version.fromString(version)
    }

    private boolean osxAarch64() {
        Architecture.current() == Architecture.AARCH64 &&
            ElasticsearchDistribution.CURRENT_PLATFORM.equals(ElasticsearchDistribution.Platform.DARWIN)
    }

    private List<Version> osFiltered(ArrayList<Version> versions) {
        return osxAarch64() ? versions.findAll {it.onOrAfter("7.16.0")} : versions
    }
}
