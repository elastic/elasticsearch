/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

    import org.elasticsearch.xpack.ql.expression.Expression;
    import org.elasticsearch.xpack.ql.tree.NodeInfo;
    import org.elasticsearch.xpack.ql.tree.Source;
    import java.time.ZoneId;

public class DateFormat extends DateTimeFormat {

    public DateFormat(Source source, Expression timestamp, Expression pattern, ZoneId zoneId) {
        super(source, timestamp, pattern, zoneId);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, DateFormat::new, left(), right(), zoneId());
    }


}
