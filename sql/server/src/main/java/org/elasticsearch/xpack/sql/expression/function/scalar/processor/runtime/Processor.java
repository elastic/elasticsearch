/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime;

import org.elasticsearch.common.io.stream.NamedWriteable;

public interface Processor extends NamedWriteable {

    Object process(Object input);
}
