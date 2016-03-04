/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector;

import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.marvel.agent.exporter.MonitoringDoc;

import java.util.Collection;

public interface Collector<T> extends LifecycleComponent<T> {

    String name();

    Collection<MonitoringDoc> collect();
}