/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.analyses;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.empty;

public class TypesTests extends ESTestCase {

    public void testTypes() {
        assertThat(Sets.intersection(Types.bool(), Types.categorical()), empty());
        assertThat(Sets.intersection(Types.categorical(), Types.numerical()), empty());
        assertThat(Sets.intersection(Types.numerical(), Types.bool()), empty());
        assertThat(Sets.difference(Types.discreteNumerical(), Types.numerical()), empty());
    }
}
