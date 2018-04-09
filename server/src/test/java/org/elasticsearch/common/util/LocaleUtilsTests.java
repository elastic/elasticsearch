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

package org.elasticsearch.common.util;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.Locale;

public class LocaleUtilsTests extends ESTestCase {

    public void testIllegalLang() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> LocaleUtils.parse("yz"));
        assertThat(e.getMessage(), Matchers.containsString("Unknown language: yz"));

        e = expectThrows(IllegalArgumentException.class,
                () -> LocaleUtils.parse("yz-CA"));
        assertThat(e.getMessage(), Matchers.containsString("Unknown language: yz"));
    }

    public void testIllegalCountry() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> LocaleUtils.parse("en-YZ"));
        assertThat(e.getMessage(), Matchers.containsString("Unknown country: YZ"));

        e = expectThrows(IllegalArgumentException.class,
                () -> LocaleUtils.parse("en-YZ-foobar"));
        assertThat(e.getMessage(), Matchers.containsString("Unknown country: YZ"));
    }

    public void testIllegalNumberOfParts() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> LocaleUtils.parse("en-US-foo-bar"));
        assertThat(e.getMessage(), Matchers.containsString("Locales can have at most 3 parts but got 4"));
    }

    public void testUnderscores() {
        Locale locale1 = LocaleUtils.parse("fr_FR");
        Locale locale2 = LocaleUtils.parse("fr-FR");
        assertEquals(locale2, locale1);
    }

    public void testSimple() {
        assertEquals(Locale.FRENCH, LocaleUtils.parse("fr"));
        assertEquals(Locale.FRANCE, LocaleUtils.parse("fr-FR"));
        assertEquals(Locale.ROOT, LocaleUtils.parse("root"));
        assertEquals(Locale.ROOT, LocaleUtils.parse(""));
    }
}
