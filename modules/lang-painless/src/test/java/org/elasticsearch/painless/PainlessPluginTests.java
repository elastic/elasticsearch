/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class PainlessPluginTests extends ESTestCase {

    /**
     * The {@link PainlessPlugin#baseWhiteList()} signature is relied on from outside
     * the Elasticsearch code-base and is considered part of the plugin's external API.
     */
    public void testBaseWhiteList() {
        final List<Whitelist> baseWhiteList = PainlessPlugin.baseWhiteList();
        assertThat(baseWhiteList, is(not(empty())));
    }
}
