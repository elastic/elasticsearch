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

package org.elasticsearch.common.inject.multibindings;

import java.lang.annotation.Annotation;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jessewilson@google.com (Jesse Wilson)
 */
class RealElement implements Element {
    private static final AtomicInteger nextUniqueId = new AtomicInteger(1);

    private final int uniqueId;
    private final String setName;

    RealElement(String setName) {
        uniqueId = nextUniqueId.getAndIncrement();
        this.setName = setName;
    }

    @Override
    public String setName() {
        return setName;
    }

    @Override
    public int uniqueId() {
        return uniqueId;
    }

    @Override
    public Class<? extends Annotation> annotationType() {
        return Element.class;
    }

    @Override
    public String toString() {
        return "@" + Element.class.getName() + "(setName=" + setName + ",uniqueId=" + uniqueId + ")";
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Element && ((Element) o).setName().equals(setName()) && ((Element) o).uniqueId() == uniqueId();
    }

    @Override
    public int hashCode() {
        return 127 * ("setName".hashCode() ^ setName.hashCode()) + 127 * ("uniqueId".hashCode() ^ uniqueId);
    }
}
