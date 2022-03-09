/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.internal;

import co.elastic.logging.log4j2.EcsLayout;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.layout.AbstractStringLayout;
import org.elasticsearch.logging.api.core.Layout;

public class EcsLayoutImpl extends AbstractStringLayout implements Layout {

    private EcsLayout layout;

    public EcsLayoutImpl(EcsLayout layout) {
        super(layout.getConfiguration(), layout.getCharset(), null, null);
        this.layout = layout;
    }

    @Override
    public String toSerializable(LogEvent event) {
        return layout.toSerializable(event);
    }

    @Override
    public byte[] toByteArray(org.elasticsearch.logging.api.core.LogEvent event) {
        return layout.toByteArray((LogEvent) event);
    }
}
