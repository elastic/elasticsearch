/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.decision;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContentObject;

/**
 * Represents an autoscaling decider, a component that determines whether or not to scale.
 */
public interface AutoscalingDeciderConfiguration extends ToXContentObject, NamedWriteable {

    /**
     * The name of the autoscaling decider.
     *
     * @return the name
     */
    String name();
}
