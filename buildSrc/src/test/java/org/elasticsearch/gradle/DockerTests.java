package org.elasticsearch.gradle;

import org.junit.Test;

import static org.elasticsearch.gradle.Docker.checkVersion;
import static org.elasticsearch.gradle.Docker.extractVersion;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class DockerTests {

    @Test
    public void testExtractVersion() {
        assertThat(extractVersion("Docker version 18.06.1-ce, build e68fc7a215d7"), equalTo("18.06.1-ce"));
        assertThat(extractVersion("Docker version 17.05.0, build e68fc7a"), equalTo("17.05.0"));
        assertThat(extractVersion("Docker version 17.05.1, build e68fc7a"), equalTo("17.05.1"));
    }

    @Test
    public void testCheckAcceptableVersions() {
        assertTrue(checkVersion("18.06.1-ce"));
        assertTrue(checkVersion("17.05.1-ce"));
    }

    @Test
    public void testCheckUnacceptableVersions() {
        assertFalse(checkVersion("17.04.0"));
        assertFalse(checkVersion("16.99.0"));
    }
}
