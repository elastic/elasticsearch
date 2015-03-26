/**
 * Copyright (C) 2008 Google Inc.
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

package org.elasticsearch.common.inject.spi;

import org.elasticsearch.common.inject.Binder;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.Provider;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * A lookup of the provider for a type. Lookups are created explicitly in a module using
 * {@link org.elasticsearch.common.inject.Binder#getProvider(Class) getProvider()} statements:
 * <pre>
 *     Provider&lt;PaymentService&gt; paymentServiceProvider
 *         = getProvider(PaymentService.class);</pre>
 *
 * @author jessewilson@google.com (Jesse Wilson)
 * @since 2.0
 */
public final class ProviderLookup<T> implements Element {
    private final Object source;
    private final Key<T> key;
    private Provider<T> delegate;

    public ProviderLookup(Object source, Key<T> key) {
        this.source = checkNotNull(source, "source");
        this.key = checkNotNull(key, "key");
    }

    @Override
    public Object getSource() {
        return source;
    }

    public Key<T> getKey() {
        return key;
    }

    @Override
    public <T> T acceptVisitor(ElementVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /**
     * Sets the actual provider.
     *
     * @throws IllegalStateException if the delegate is already set
     */
    public void initializeDelegate(Provider<T> delegate) {
        checkState(this.delegate == null, "delegate already initialized");
        this.delegate = checkNotNull(delegate, "delegate");
    }

    @Override
    public void applyTo(Binder binder) {
        initializeDelegate(binder.withSource(getSource()).getProvider(key));
    }

    /**
     * Returns the delegate provider, or {@code null} if it has not yet been initialized. The delegate
     * will be initialized when this element is processed, or otherwise used to create an injector.
     */
    public Provider<T> getDelegate() {
        return delegate;
    }

    /**
     * Returns the looked up provider. The result is not valid until this lookup has been initialized,
     * which usually happens when the injector is created. The provider will throw an {@code
     * IllegalStateException} if you try to use it beforehand.
     */
    public Provider<T> getProvider() {
        return new Provider<T>() {
            @Override
            public T get() {
                checkState(delegate != null,
                        "This Provider cannot be used until the Injector has been created.");
                return delegate.get();
            }

            @Override
            public String toString() {
                return "Provider<" + key.getTypeLiteral() + ">";
            }
        };
    }
}
