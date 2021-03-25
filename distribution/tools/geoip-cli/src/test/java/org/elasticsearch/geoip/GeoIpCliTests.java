/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.geoip;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Matchers;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GeoIpCliTests extends ESTestCase {

    public void test() throws Exception {
        GeoIpCli cli = new GeoIpCli();
        MockTerminal terminal = new MockTerminal();
        OptionSet optionSet = mock(OptionSet.class);
        when(optionSet.valueOf(Matchers.<OptionSpec<String>>anyObject())).thenReturn("/Users/hesperus/work/elasticsearch/test/fixtures" +
            "/geoip-fixture/src/main/resources");
        cli.execute(terminal, optionSet);
    }
}
