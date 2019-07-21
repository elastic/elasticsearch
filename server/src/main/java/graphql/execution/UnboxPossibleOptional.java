package graphql.execution;

import graphql.Internal;

import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;

@Internal
public class UnboxPossibleOptional {

    public static Object unboxPossibleOptional(Object result) {
        if (result instanceof Optional) {
            Optional optional = (Optional) result;
            if (optional.isPresent()) {
                return optional.get();
            } else {
                return null;
            }
        } else if (result instanceof OptionalInt) {
            OptionalInt optional = (OptionalInt) result;
            if (optional.isPresent()) {
                return optional.getAsInt();
            } else {
                return null;
            }
        } else if (result instanceof OptionalDouble) {
            OptionalDouble optional = (OptionalDouble) result;
            if (optional.isPresent()) {
                return optional.getAsDouble();
            } else {
                return null;
            }
        } else if (result instanceof OptionalLong) {
            OptionalLong optional = (OptionalLong) result;
            if (optional.isPresent()) {
                return optional.getAsLong();
            } else {
                return null;
            }
        }

        return result;
    }
}
