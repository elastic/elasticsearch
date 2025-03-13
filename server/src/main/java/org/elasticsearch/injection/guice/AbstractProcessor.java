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

package org.elasticsearch.injection.guice;

import org.elasticsearch.injection.guice.internal.Errors;
import org.elasticsearch.injection.guice.spi.Element;
import org.elasticsearch.injection.guice.spi.ElementVisitor;
import org.elasticsearch.injection.guice.spi.Message;
import org.elasticsearch.injection.guice.spi.ProviderLookup;

import java.util.Iterator;
import java.util.List;

/**
 * Abstract base class for creating an injector from module elements.
 * <p>
 * Extending classes must return {@code true} from any overridden
 * {@code visit*()} methods, in order for the element processor to remove the
 * handled element.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 */
abstract class AbstractProcessor implements ElementVisitor<Boolean> {

    protected Errors errors;
    protected InjectorImpl injector;

    protected AbstractProcessor(Errors errors) {
        this.errors = errors;
    }

    public void process(Iterable<InjectorShell> isolatedInjectorBuilders) {
        for (InjectorShell injectorShell : isolatedInjectorBuilders) {
            process(injectorShell.getInjector(), injectorShell.getElements());
        }
    }

    public void process(InjectorImpl injector, List<Element> elements) {
        Errors errorsAnyElement = this.errors;
        this.injector = injector;
        try {
            for (Iterator<Element> i = elements.iterator(); i.hasNext();) {
                Element element = i.next();
                this.errors = errorsAnyElement.withSource(element.getSource());
                Boolean allDone = element.acceptVisitor(this);
                if (allDone) {
                    i.remove();
                }
            }
        } finally {
            this.errors = errorsAnyElement;
            this.injector = null;
        }
    }

    @Override
    public Boolean visit(Message message) {
        return false;
    }

    @Override
    public <T> Boolean visit(Binding<T> binding) {
        return false;
    }

    @Override
    public <T> Boolean visit(ProviderLookup<T> providerLookup) {
        return false;
    }
}
