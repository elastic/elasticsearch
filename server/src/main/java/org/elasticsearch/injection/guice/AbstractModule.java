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

package org.elasticsearch.injection.guice;

import org.elasticsearch.injection.guice.binder.AnnotatedBindingBuilder;

import java.util.Objects;

/**
 * A support class for {@link Module}s which reduces repetition and results in
 * a more readable configuration. Simply extend this class, implement {@link
 * #configure()}, and call the inherited methods which mirror those found in
 * {@link Binder}. For example:
 * <pre>
 * public class MyModule extends AbstractModule {
 *   protected void configure() {
 *     bind(Service.class).to(ServiceImpl.class).in(Singleton.class);
 *     bind(CreditCardPaymentService.class);
 *     bind(PaymentService.class).to(CreditCardPaymentService.class);
 *     bindConstant().annotatedWith(Names.named("port")).to(8080);
 *   }
 * }
 * </pre>
 *
 * @author crazybob@google.com (Bob Lee)
 */
public abstract class AbstractModule implements Module {

    Binder binder;

    @Override
    public final synchronized void configure(Binder builder) {
        if (this.binder != null) {
            throw new IllegalStateException("Re-entry is not allowed.");
        }
        this.binder = Objects.requireNonNull(builder, "builder");
        try {
            configure();
        } finally {
            this.binder = null;
        }
    }

    /**
     * Configures a {@link Binder} via the exposed methods.
     */
    protected abstract void configure();

    /**
     * Gets direct access to the underlying {@code Binder}.
     */
    protected Binder binder() {
        return binder;
    }

    /**
     * @see Binder#bind(TypeLiteral)
     */
    protected <T> AnnotatedBindingBuilder<T> bind(TypeLiteral<T> typeLiteral) {
        return binder.bind(typeLiteral);
    }

    /**
     * @see Binder#bind(Class)
     */
    protected <T> AnnotatedBindingBuilder<T> bind(Class<T> clazz) {
        return binder.bind(clazz);
    }

}
