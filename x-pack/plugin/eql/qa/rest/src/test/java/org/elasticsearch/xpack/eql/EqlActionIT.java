/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql;

import org.elasticsearch.test.eql.CommonEqlActionTestCase;
import org.elasticsearch.test.eql.EqlSpec;

public class EqlActionIT extends CommonEqlActionTestCase {
    // Constructor for parameterized test
    public EqlActionIT(int num, String name, EqlSpec spec) {
        super(num, name, spec);
    }
}
