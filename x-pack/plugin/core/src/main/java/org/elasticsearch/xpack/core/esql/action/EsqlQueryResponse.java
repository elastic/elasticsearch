/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.esql.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.core.Releasable;

import java.util.Iterator;
import java.util.List;

public abstract class EsqlQueryResponse extends ActionResponse implements Releasable {

    public abstract List<? extends ColumnInfo> columns();

    public abstract Iterator<Iterator<Object>> values();

    public abstract boolean columnar();
}
