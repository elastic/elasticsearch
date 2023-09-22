/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;

/**
 * Allows to inject instances of a breaker and bigArrays into the Global block factory.
 * The Global factory is somewhat temporary, therefore this interface and its ServiceLoader
 * machinery can be removed once the Global factory is removed.
 */
public interface BlockFactoryParameters {

    CircuitBreaker breaker();

    BigArrays bigArrays();
}
