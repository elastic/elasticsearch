/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.persistent.decider;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.persistent.PersistentTaskParams;

/**
 * {@link AssignmentDecider} are used to decide if a persistent
 * task can be assigned to a node.
 */
public abstract class AssignmentDecider<Params extends PersistentTaskParams> extends AbstractComponent {

    public AssignmentDecider(Settings settings) {
        super(settings);
    }

    /**
     * Returns a {@link AssignmentDecision} whether the given persistent task can be assigned
     * to any node of the cluster. The default is {@link AssignmentDecision#ALWAYS}.
     *
     * @param taskName   the task's name
     * @param taskParams the task's parameters
     * @return the {@link AssignmentDecision}
     */
    public AssignmentDecision canAssign(final String taskName, final @Nullable Params taskParams) {
        return AssignmentDecision.ALWAYS;
    }
}
