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

package org.elasticsearch.painless;

import org.antlr.v4.runtime.ParserRuleContext;
import org.elasticsearch.painless.Definition.Cast;
import org.elasticsearch.painless.Definition.Transform;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.Metadata.ExpressionMetadata;
import org.objectweb.asm.commons.GeneratorAdapter;

class WriterCaster {
    private final GeneratorAdapter execute;

    WriterCaster(final GeneratorAdapter execute) {
        this.execute = execute;
    }

    void checkWriteCast(final ExpressionMetadata sort) {
        checkWriteCast(sort.source, sort.cast);
    }

    void checkWriteCast(final ParserRuleContext source, final Cast cast) {
        if (cast instanceof Transform) {
            writeTransform((Transform)cast);
        } else if (cast != null) {
            writeCast(cast);
        } else {
            throw new IllegalStateException(WriterUtility.error(source) + "Unexpected cast object.");
        }
    }

    private void writeCast(final Cast cast) {
        final Type from = cast.from;
        final Type to = cast.to;

        if (from.equals(to)) {
            return;
        }

        if (from.sort.numeric && from.sort.primitive && to.sort.numeric && to.sort.primitive) {
            execute.cast(from.type, to.type);
        } else {
            try {
                from.clazz.asSubclass(to.clazz);
            } catch (ClassCastException exception) {
                execute.checkCast(to.type);
            }
        }
    }

    private void writeTransform(final Transform transform) {
        if (transform.upcast != null) {
            execute.checkCast(transform.upcast.type);
        }

        if (java.lang.reflect.Modifier.isStatic(transform.method.reflect.getModifiers())) {
            execute.invokeStatic(transform.method.owner.type, transform.method.method);
        } else if (java.lang.reflect.Modifier.isInterface(transform.method.owner.clazz.getModifiers())) {
            execute.invokeInterface(transform.method.owner.type, transform.method.method);
        } else {
            execute.invokeVirtual(transform.method.owner.type, transform.method.method);
        }

        if (transform.downcast != null) {
            execute.checkCast(transform.downcast.type);
        }
    }
}
