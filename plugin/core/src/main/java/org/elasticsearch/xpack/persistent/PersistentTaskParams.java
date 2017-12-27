/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContentObject;

/**
 * Parameters used to start persistent task
 */
public interface PersistentTaskParams extends NamedWriteable, ToXContentObject {

}
