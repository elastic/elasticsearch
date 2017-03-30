/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
