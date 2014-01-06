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
package org.elasticsearch.search.highlight;

import com.google.common.collect.Lists;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.Multibinder;

import java.util.List;

/**
 *
 */
public class HighlightModule extends AbstractModule {

    private List<Class<? extends Highlighter>> highlighters = Lists.newArrayList();

    public HighlightModule() {
        registerHighlighter(FastVectorHighlighter.class);
        registerHighlighter(PlainHighlighter.class);
        registerHighlighter(PostingsHighlighter.class);
    }

    public void registerHighlighter(Class<? extends Highlighter> clazz) {
        highlighters.add(clazz);
    }

    @Override
    protected void configure() {
        Multibinder<Highlighter> multibinder = Multibinder.newSetBinder(binder(), Highlighter.class);
        for (Class<? extends Highlighter> highlighter : highlighters) {
            multibinder.addBinding().to(highlighter);
        }
        bind(Highlighters.class).asEagerSingleton();
    }
}
