/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.apache.lucene.util.Constants;
import org.elasticsearch.test.ESTestCase;

import java.util.Locale;

public class PluginsTests extends ESTestCase {

    public void testMakePlatformName() {
        final String platformName = Platforms.platformName(Constants.OS_NAME, Constants.OS_ARCH);

        assertFalse(platformName, platformName.isEmpty());
        assertTrue(platformName, platformName.equals(platformName.toLowerCase(Locale.ROOT)));
        assertTrue(platformName, platformName.indexOf("-") > 0);
        assertTrue(platformName, platformName.indexOf("-") < platformName.length() - 1);
        assertFalse(platformName, platformName.contains(" "));
    }

    public void testMakeSpecificPlatformNames() {
        assertEquals("darwin-x86_64", Platforms.platformName("Mac OS X", "x86_64"));
        assertEquals("linux-x86_64", Platforms.platformName("Linux", "amd64"));
        assertEquals("linux-x86", Platforms.platformName("Linux", "i386"));
        assertEquals("windows-x86_64", Platforms.platformName("Windows Server 2008 R2", "amd64"));
        assertEquals("windows-x86", Platforms.platformName("Windows Server 2008", "x86"));
        assertEquals("windows-x86_64", Platforms.platformName("Windows 8.1", "amd64"));
        assertEquals("sunos-x86_64", Platforms.platformName("SunOS", "amd64"));
    }

}
