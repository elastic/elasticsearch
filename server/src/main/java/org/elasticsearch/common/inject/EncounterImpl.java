/*
 * Copyright (C) 2009 Google Inc.
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
import org.elasticsearch.common.inject.spi.InjectionListener;
import org.elasticsearch.common.inject.spi.Message;
import org.elasticsearch.common.inject.spi.TypeEncounter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author jessewilson@google.com (Jesse Wilson)
 */
final class EncounterImpl<T> implements TypeEncounter<T> {

    private final Errors errors;
    private final Lookups lookups;
    private List<MembersInjector<? super T>> membersInjectors; // lazy
    private List<InjectionListener<? super T>> injectionListeners; // lazy
    private boolean valid = true;

    EncounterImpl(Errors errors, Lookups lookups) {
        this.errors = errors;
        this.lookups = lookups;
    }

    public void invalidate() {
        valid = false;
    }

    public List<MembersInjector<? super T>> getMembersInjectors() {
        return membersInjectors == null
                ? Collections.<MembersInjector<? super T>>emptyList()
                : Collections.unmodifiableList(membersInjectors);
    }

    public List<InjectionListener<? super T>> getInjectionListeners() {
        return injectionListeners == null
                ? Collections.<InjectionListener<? super T>>emptyList()
                : Collections.unmodifiableList(injectionListeners);
    }

    @Override
    public void register(MembersInjector<? super T> membersInjector) {
        if (valid == false) {
            throw new IllegalStateException("Encounters may not be used after hear() returns.");
        }

        if (membersInjectors == null) {
            membersInjectors = new ArrayList<>();
        }

        membersInjectors.add(membersInjector);
    }

    @Override
    public void register(InjectionListener<? super T> injectionListener) {
        if (valid == false) {
            throw new IllegalStateException("Encounters may not be used after hear() returns.");
        }

        if (injectionListeners == null) {
            injectionListeners = new ArrayList<>();
        }

        injectionListeners.add(injectionListener);
    }

    @Override
    public void addError(String message, Object... arguments) {
        if (valid == false) {
            throw new IllegalStateException("Encounters may not be used after hear() returns.");
        }
        errors.addMessage(message, arguments);
    }

    @Override
    public void addError(Throwable t) {
        if (valid == false) {
            throw new IllegalStateException("Encounters may not be used after hear() returns.");
        }
        errors.errorInUserCode(t, "An exception was caught and reported. Message: %s", t.getMessage());
    }

    @Override
    public void addError(Message message) {
        if (valid == false) {
            throw new IllegalStateException("Encounters may not be used after hear() returns.");
        }
        errors.addMessage(message);
    }

    @Override
    public <T> Provider<T> getProvider(Key<T> key) {
        if (valid == false) {
            throw new IllegalStateException("Encounters may not be used after hear() returns.");
        }
        return lookups.getProvider(key);
    }

    @Override
    public <T> Provider<T> getProvider(Class<T> type) {
        return getProvider(Key.get(type));
    }

    @Override
    public <T> MembersInjector<T> getMembersInjector(TypeLiteral<T> typeLiteral) {
        if (valid == false) {
            throw new IllegalStateException("Encounters may not be used after hear() returns.");
        }
        return lookups.getMembersInjector(typeLiteral);
    }

    @Override
    public <T> MembersInjector<T> getMembersInjector(Class<T> type) {
        return getMembersInjector(TypeLiteral.get(type));
    }
}
