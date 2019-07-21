package graphql;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.CONSTRUCTOR;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;

/**
 * This represents code that the graphql-java project considers internal code that MAY not be stable within
 * major releases.
 *
 * In general unnecessary changes will be avoided but you should not depend on internal classes being stable
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(value = {CONSTRUCTOR, METHOD, TYPE})
public @interface Internal {
}
