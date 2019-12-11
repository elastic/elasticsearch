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

package org.elasticsearch.client.ilm;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.spi.NamedXContentProvider;

import java.util.Arrays;
import java.util.List;

public class IndexLifecycleNamedXContentProvider implements NamedXContentProvider {


    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        return Arrays.asList(
            // ILM
            new NamedXContentRegistry.Entry(LifecycleAction.class,
                new ParseField(AllocateAction.NAME),
                AllocateAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class,
                new ParseField(DeleteAction.NAME),
                DeleteAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class,
                new ParseField(ForceMergeAction.NAME),
                ForceMergeAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class,
                new ParseField(ReadOnlyAction.NAME),
                ReadOnlyAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class,
                new ParseField(RolloverAction.NAME),
                RolloverAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class,
                new ParseField(ShrinkAction.NAME),
                ShrinkAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class,
                new ParseField(FreezeAction.NAME),
                FreezeAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class,
                new ParseField(SetPriorityAction.NAME),
                SetPriorityAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class,
                new ParseField(UnfollowAction.NAME),
                UnfollowAction::parse)
        );
    }
}
