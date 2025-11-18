/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.tasks.TaskAwareRequest;

import java.net.InetSocketAddress;

public interface TransportRequest extends Writeable, RefCounted, TaskAwareRequest {
    void remoteAddress(InetSocketAddress remoteAddress);

    InetSocketAddress remoteAddress();
}
