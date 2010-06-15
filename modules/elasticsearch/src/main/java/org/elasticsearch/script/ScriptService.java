/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.script;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.math.UnboxedMathUtils;
import org.elasticsearch.common.mvel2.MVEL;
import org.elasticsearch.common.mvel2.ParserContext;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * @author kimchy (shay.banon)
 */
public class ScriptService extends AbstractComponent {

    private final ConcurrentMap<String, Object> cache = ConcurrentCollections.newConcurrentMap();

    private final ParserContext parserContext;

    @Inject public ScriptService(Settings settings) {
        super(settings);

        parserContext = new ParserContext();
        parserContext.addPackageImport("java.util");
        parserContext.addPackageImport("org.elasticsearch.util.gnu.trove");
        parserContext.addPackageImport("org.elasticsearch.common.joda");
        parserContext.addImport("time", MVEL.getStaticMethod(System.class, "currentTimeMillis", new Class[0]));
        // unboxed version of Math, better performance since conversion from boxed to unboxed my mvel is not needed
        for (Method m : UnboxedMathUtils.class.getMethods()) {
            if ((m.getModifiers() & Modifier.STATIC) > 0) {
                parserContext.addImport(m.getName(), m);
            }
        }
    }

    public Object compile(String script) {
        Object compiled = cache.get(script);
        if (compiled != null) {
            return compiled;
        }
        synchronized (cache) {
            compiled = cache.get(script);
            if (compiled != null) {
                return compiled;
            }
            compiled = MVEL.compileExpression(script, parserContext);
            cache.put(script, compiled);
        }
        return compiled;
    }

    public Object execute(Object script, Map vars) {
        return MVEL.executeExpression(script, vars);
    }

    public void clear() {
        cache.clear();
    }
}
