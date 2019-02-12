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

package org.elasticsearch.common.inject.internal;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Lazily creates (and caches) values for keys. If creating the value fails (with errors), an
 * exception is thrown on retrieval.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 */
public abstract class FailableCache<K, V> {

    private final ConcurrentHashMap<K, Object> cache = new ConcurrentHashMap<>();

    protected abstract V create(K key, Errors errors) throws ErrorsException;

    public V get(K key, Errors errors) throws ErrorsException {
        Object resultOrError = cache.get(key);
        if (resultOrError == null) {
            synchronized (this) {
                resultOrError = load(key);
                // we can't use cache.computeIfAbsent since this might be recursively call this API
                cache.putIfAbsent(key, resultOrError);
            }
        }
        if (resultOrError instanceof Errors) {
            errors.merge((Errors) resultOrError);
            throw errors.toException();
        } else {
            @SuppressWarnings("unchecked") // create returned a non-error result, so this is safe
            V result = (V) resultOrError;
            return result;
        }
    }


    private Object load(K key) {
        Errors errors = new Errors();
        V result = null;
        try {
            result = create(key, errors);
        } catch (ErrorsException e) {
            errors.merge(e.getErrors());
        }
        return errors.hasErrors() ? errors : result;
    }
}
