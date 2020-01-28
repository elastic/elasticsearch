/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.time.ZonedDateTime;

public class CurrentDate extends CurrentFunction<ZonedDateTime> {

    public CurrentDate(Source source, Configuration configuration) {
        super(source, configuration, DateUtils.asDateOnly(configuration.now()), SqlDataTypes.DATE);
    }

    @Override
    protected NodeInfo<CurrentDate> info() {
        return NodeInfo.create(this, CurrentDate::new, configuration());
    }
}
