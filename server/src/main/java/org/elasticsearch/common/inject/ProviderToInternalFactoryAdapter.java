/*
 * Copyright (C) 2006 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.common.inject;

import org.elasticsearch.common.inject.internal.Errors;
import org.elasticsearch.common.inject.internal.ErrorsException;
import org.elasticsearch.common.inject.internal.InternalFactory;

/**
 * @author crazybob@google.com (Bob Lee)
 */
class ProviderToInternalFactoryAdapter<T> implements Provider<T> {

    private final InjectorImpl injector;
    private final InternalFactory<? extends T> internalFactory;

    ProviderToInternalFactoryAdapter(InjectorImpl injector, InternalFactory<? extends T> internalFactory) {
        this.injector = injector;
        this.internalFactory = internalFactory;
    }

    @Override
    public T get() {
        final Errors errors = new Errors();
        try {
            T t = injector.callInContext((ContextualCallable<T>) context -> internalFactory.get(errors, context, context.getDependency()));
            errors.throwIfNewErrors(0);
            return t;
        } catch (ErrorsException e) {
            throw new ProvisionException(errors.merge(e.getErrors()).getMessages());
        }
    }

    @Override
    public String toString() {
        return internalFactory.toString();
    }
}
