/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.group;

import java.util.List;

import com.google.common.collect.Lists;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.search.group.terms.TermsGroupProcessor;

/**
 *
 */
public class GroupModule extends AbstractModule {

    private List<Class<? extends GroupProcessor>> processors = Lists.newArrayList();

    public GroupModule() {
        processors.add(TermsGroupProcessor.class);
    }

    public void addGroupProcessor(Class<? extends GroupProcessor> groupProcessor) {
        processors.add(groupProcessor);
    }

    @Override
    protected void configure() {
        Multibinder<GroupProcessor> multibinder = Multibinder.newSetBinder(binder(), GroupProcessor.class);
        for (Class<? extends GroupProcessor> processor : processors) {
            multibinder.addBinding().to(processor);
        }
        bind(GroupProcessors.class).asEagerSingleton();
        bind(GroupParseElement.class).asEagerSingleton();
    }
}
