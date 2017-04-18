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

package org.elasticsearch.painless.node;

import org.elasticsearch.painless.antlr.Walker;

/**
 * Implemented by {@link ANode}s that might be "null safe" like {@link PField} and {@link PCallInvoke}. Only implemented by
 * {@linkplain ANodes}s that are returned by the {@link Walker} so nodes like {@link PSubNullSafeCallInvoke} don't implement it
 */
public interface IMaybeNullSafe {
    /**
     * If the {@code node} implements {@linkplain IMaybeNullSafe} and is null safe then set {@code defaultForNull} so it can jump to the
     * {@linkplain EElvis}'s right hand side rather than emit {@code null}.
     */
    static boolean applyIfPossible(ANode node, EElvis defaultForNull) {
        if (node instanceof IMaybeNullSafe) {
            IMaybeNullSafe maybeNullSafe = (IMaybeNullSafe) node;
            if (maybeNullSafe.isNullSafe()) {
                maybeNullSafe.setDefaultForNull(defaultForNull);
                return true;
            }
        }
        return false;
    }

    /**
     * Has this node been configured to be null safe? Nodes like {@link PField} aren't null safe if specified like {@code params.a} but are
     * if specified like {@code params?.a}.
     */
    boolean isNullSafe();

    /**
     * Set {@code defaultForNull} so this node can jump to the {@linkplain EElvis}'s right hand side rather than emit {@code null}.
     */
    void setDefaultForNull(EElvis defaultForNull);
}
