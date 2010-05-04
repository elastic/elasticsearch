/*
 * Copyright (C) 2007 Google Inc.
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

package org.elasticsearch.util.base;

import org.elasticsearch.util.annotations.GwtCompatible;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Map;

import static org.elasticsearch.util.base.Preconditions.*;

/**
 * Useful functions.
 *
 * <p>All methods returns serializable functions as long as they're given
 * serializable parameters.
 *
 * @author Mike Bostock
 * @author Vlad Patryshev
 * @author Jared Levy
 */
@GwtCompatible
public final class Functions {
  private Functions() {}

  /**
   * Returns a function that calls {@code toString()} on its argument. The
   * function does not accept nulls; it will throw a
   * {@link NullPointerException} when applied to {@code null}.
   */
  public static Function<Object, String> toStringFunction() {
    return ToStringFunction.INSTANCE;
  }

  // enum singleton pattern
  private enum ToStringFunction implements Function<Object, String> {
    INSTANCE;

    public String apply(Object o) {
      return o.toString();
    }

    @Override public String toString() {
      return "toString";
    }
  }

  /**
   * Returns the identity function.
   */
  @SuppressWarnings("unchecked")
  public static <E> Function<E, E> identity() {
    return (Function<E, E>) IdentityFunction.INSTANCE;
  }

  // enum singleton pattern
  private enum IdentityFunction implements Function<Object, Object> {
    INSTANCE;

    public Object apply(Object o) {
      return o;
    }

    @Override public String toString() {
      return "identity";
    }
  }

  /**
   * Returns a function which performs a map lookup. The returned function
   * throws an {@link IllegalArgumentException} if given a key that does not
   * exist in the map.
   */
  public static <K, V> Function<K, V> forMap(Map<K, V> map) {
    return new FunctionForMapNoDefault<K, V>(map);
  }

  private static class FunctionForMapNoDefault<K, V>
      implements Function<K, V>, Serializable {
    final Map<K, V> map;

    FunctionForMapNoDefault(Map<K, V> map) {
      this.map = checkNotNull(map);
    }
    public V apply(K key) {
      V result = map.get(key);
      checkArgument(result != null || map.containsKey(key),
          "Key '%s' not present in map", key);
      return result;
    }
    @Override public boolean equals(Object o) {
      if (o instanceof FunctionForMapNoDefault) {
        FunctionForMapNoDefault<?, ?> that = (FunctionForMapNoDefault<?, ?>) o;
        return map.equals(that.map);
      }
      return false;
    }
    @Override public int hashCode() {
      return map.hashCode();
    }
    @Override public String toString() {
      return "forMap(" + map + ")";
    }
    private static final long serialVersionUID = 0;
  }

  /**
   * Returns a function which performs a map lookup with a default value. The
   * function created by this method returns {@code defaultValue} for all
   * inputs that do not belong to the map's key set.
   *
   * @param map source map that determines the function behavior
   * @param defaultValue the value to return for inputs that aren't map keys
   * @return function that returns {@code map.get(a)} when {@code a} is a key,
   *     or {@code defaultValue} otherwise
   */
  public static <K, V> Function<K, V> forMap(
      Map<K, ? extends V> map, @Nullable V defaultValue) {
    return new ForMapWithDefault<K, V>(map, defaultValue);
  }

  private static class ForMapWithDefault<K, V>
      implements Function<K, V>, Serializable {
    final Map<K, ? extends V> map;
    final V defaultValue;

    ForMapWithDefault(Map<K, ? extends V> map, V defaultValue) {
      this.map = checkNotNull(map);
      this.defaultValue = defaultValue;
    }
    public V apply(K key) {
      return map.containsKey(key) ? map.get(key) : defaultValue;
    }
    @Override public boolean equals(Object o) {
      if (o instanceof ForMapWithDefault) {
        ForMapWithDefault<?, ?> that = (ForMapWithDefault<?, ?>) o;
        return map.equals(that.map)
            && Objects.equal(defaultValue, that.defaultValue);
      }
      return false;
    }
    @Override public int hashCode() {
      return Objects.hashCode(map, defaultValue);
    }
    @Override public String toString() {
      return "forMap(" + map + ", defaultValue=" + defaultValue + ")";
    }
    private static final long serialVersionUID = 0;
  }

  /**
   * Returns the composition of two functions. For {@code f: A->B} and
   * {@code g: B->C}, composition is defined as the function h such that
   * {@code h(a) == g(f(a))} for each {@code a}.
   *
   * @see <a href="//en.wikipedia.org/wiki/Function_composition">
   * function composition</a>
   *
   * @param g the second function to apply
   * @param f the first function to apply
   * @return the composition of {@code f} and {@code g}
   */
  public static <A, B, C> Function<A, C> compose(
      Function<B, C> g, Function<A, ? extends B> f) {
    return new FunctionComposition<A, B, C>(g, f);
  }

  private static class FunctionComposition<A, B, C>
      implements Function<A, C>, Serializable {
    private final Function<B, C> g;
    private final Function<A, ? extends B> f;

    public FunctionComposition(Function<B, C> g,
        Function<A, ? extends B> f) {
      this.g = checkNotNull(g);
      this.f = checkNotNull(f);
    }
    public C apply(A a) {
      return g.apply(f.apply(a));
    }
    @Override public boolean equals(Object obj) {
      if (obj instanceof FunctionComposition) {
        FunctionComposition<?, ?, ?> that = (FunctionComposition<?, ?, ?>) obj;
        return f.equals(that.f) && g.equals(that.g);
      }
      return false;
    }

    @Override public int hashCode() {
      return f.hashCode() ^ g.hashCode();
    }
    @Override public String toString() {
      return g.toString() + "(" + f.toString() + ")";
    }
    private static final long serialVersionUID = 0;
  }

  /**
   * Creates a function that returns the same boolean output as the given
   * predicate for all inputs.
   */
  public static <T> Function<T, Boolean> forPredicate(Predicate<T> predicate) {
    return new PredicateFunction<T>(predicate);
  }

  /** @see Functions#forPredicate */
  private static class PredicateFunction<T>
      implements Function<T, Boolean>, Serializable {
    private final Predicate<T> predicate;

    private PredicateFunction(Predicate<T> predicate) {
      this.predicate = checkNotNull(predicate);
    }

    public Boolean apply(T t) {
      return predicate.apply(t);
    }
    @Override public boolean equals(Object obj) {
      if (obj instanceof PredicateFunction) {
        PredicateFunction<?> that = (PredicateFunction<?>) obj;
        return predicate.equals(that.predicate);
      }
      return false;
    }
    @Override public int hashCode() {
      return predicate.hashCode();
    }
    @Override public String toString() {
      return "forPredicate(" + predicate + ")";
    }
    private static final long serialVersionUID = 0;
  }

  /**
   * Creates a function that returns {@code value} for any input.
   *
   * @param value the constant value for the function to return
   * @return a function that always returns {@code value}
   */
  public static <E> Function<Object, E> constant(@Nullable E value) {
    return new ConstantFunction<E>(value);
  }

  private static class ConstantFunction<E>
      implements Function<Object, E>, Serializable {
    private final E value;

    public ConstantFunction(@Nullable E value) {
      this.value = value;
    }
    public E apply(Object from) {
      return value;
    }
    @Override public boolean equals(Object obj) {
      if (obj instanceof ConstantFunction) {
        ConstantFunction<?> that = (ConstantFunction<?>) obj;
        return Objects.equal(value, that.value);
      }
      return false;
    }
    @Override public int hashCode() {
      return (value == null) ? 0 : value.hashCode();
    }
    @Override public String toString() {
      return "constant(" + value + ")";
    }
    private static final long serialVersionUID = 0;
  }
}
