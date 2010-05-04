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

package org.elasticsearch.util.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The presence of this annotation on a type indicates that the type may be
 * used with the
 * <a href="http://code.google.com/webtoolkit/">Google Web Toolkit</a> (GWT).
 * When applied to a method, the return type of the method is GWT compatible.
 * It's useful to indicate that an instance created by factory methods has a GWT
 * serializable type.  In the following example,
 *
 * <pre style="code">
 * {@literal @}GwtCompatible
 * class Lists {
 *   ...
 *   {@literal @}GwtCompatible(serializable = true)
 *   static &lt;E> List&lt;E> newArrayList(E... elements) {
 *     ...
 *   }
 * }
 * </pre>
 * The return value of {@code Lists.newArrayList(E[])} has GWT
 * serializable type.  It is also useful in specifying contracts of interface
 * methods.  In the following example,
 *
 * <pre style="code">
 * {@literal @}GwtCompatible
 * interface ListFactory {
 *   ...
 *   {@literal @}GwtCompatible(serializable = true)
 *   &lt;E> List&lt;E> newArrayList(E... elements);
 * }
 * </pre>
 * The {@code newArrayList(E[])} method of all implementations of {@code
 * ListFactory} is expected to return a value with a GWT serializable type.
 *
 * <p>Note that a {@code GwtCompatible} type may have some {@link
 * GwtIncompatible} methods.
 *
 * @author Charles Fry
 * @author Hayward Chan
 */
@Retention(RetentionPolicy.CLASS)
@Target({ ElementType.TYPE, ElementType.METHOD })
// @Documented - uncomment when GWT support is official
@GwtCompatible
public @interface GwtCompatible {

  /**
   * When {@code true}, the annotated type or the type of the method return
   * value is GWT serializable.
   *
   * @see <a href="http://code.google.com/docreader/#p=google-web-toolkit-doc-1-5&t=DevGuideSerializableTypes">
   *     Documentation about GWT serialization</a>
   */
  boolean serializable() default false;

  /**
   * When {@code true}, the annotated type is emulated in GWT. The emulated
   * source (also known as super-source) is different from the implementation
   * used by the JVM.
   *
   * @see <a href="http://code.google.com/docreader/#p=google-web-toolkit-doc-1-5&t=DevGuideModuleXml">
   *     Documentation about GWT emulated source</a>
   */
  boolean emulated() default false;
}
