/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.xpack.eql.session.EqlConfiguration;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.tree.Source;

public class StartsWith extends org.elasticsearch.xpack.ql.expression.function.scalar.string.StartsWith {

    public StartsWith(Source source, Expression field, Expression pattern, Configuration configuration) {
        super(source, field, pattern, configuration);
    }

    @Override
    public boolean isCaseSensitive() {
        return ((EqlConfiguration) configuration()).isCaseSensitive();
    }

}
