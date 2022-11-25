/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.example.analysis;


import org.elasticsearch.plugin.analysis.api.CharFilterFactory;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class ExampleCharFilterFactoryTests {
    @Test
    public void exampleCharFilterIsAnnotatedWithName() {
        CharFilterFactory charFilterFactory = new ExampleCharFilterFactory();
        assertThat(charFilterFactory.name(), equalTo("example_char_filter"));
    }
}
