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

package org.elasticsearch.util.gcommon.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The presence of this annotation on a method indicates that the method may
 * <em>not</em> be used with the
 * <a href="http://code.google.com/webtoolkit/">Google Web Toolkit</a> (GWT),
 * even though its type is annotated as {@link GwtCompatible} and accessible in
 * GWT.  They can cause GWT compilation errors or simply unexpected exceptions
 * when used in GWT.
 *
 * <p>Note that this annotation should only be applied to methods of types which
 * are annotated as {@link GwtCompatible}.
 *
 * @author Charles Fry
 */
@Retention(RetentionPolicy.CLASS)
@Target(ElementType.METHOD)
// @Documented - uncomment when GWT support is official
@GwtCompatible
public @interface GwtIncompatible {

  /**
   * Describes why the annotated element is incompatible with GWT. Since this is
   * generally due to a dependence on a type/method which GWT doesn't support,
   * it is sufficient to simply reference the unsupported type/method. E.g.
   * "Class.isInstance".
   */
  String value();

}
