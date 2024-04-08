/*
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

import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.Provider;

import java.util.Objects;

/**
 * A lookup of the provider for a type.
 * <pre>
 *     Provider&lt;PaymentService&gt; paymentServiceProvider
 *         = getProvider(PaymentService.class);</pre>
 *
 * @author jessewilson@google.com (Jesse Wilson)
 * @since 2.0
 */
public final class ProviderLookup<T> implements Element {

    // NOTE: this class is not part of guice and was added so the provider lookup's key can be accessible for tests
    public static class ProviderImpl<T> implements Provider<T> {
        private final ProviderLookup<T> lookup;

        private ProviderImpl(ProviderLookup<T> lookup) {
            this.lookup = lookup;
        }

        @Override
        public T get() {
            if (lookup.delegate == null) {
                throw new IllegalStateException("This Provider cannot be used until the Injector has been created.");
            }
            return lookup.delegate.get();
        }

        @Override
        public String toString() {
            return "Provider<" + lookup.key.getTypeLiteral() + ">";
        }

    }

    private final Object source;
    private final Key<T> key;
    private Provider<T> delegate;

    public ProviderLookup(Object source, Key<T> key) {
        this.source = Objects.requireNonNull(source, "source");
        this.key = Objects.requireNonNull(key, "key");
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
        if (this.delegate != null) {
            throw new IllegalStateException("delegate already initialized");
        }
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    /**
     * Returns the looked up provider. The result is not valid until this lookup has been initialized,
     * which usually happens when the injector is created. The provider will throw an {@code
     * IllegalStateException} if you try to use it beforehand.
     */
    public Provider<T> getProvider() {
        return new ProviderImpl<>(this);
    }
}
