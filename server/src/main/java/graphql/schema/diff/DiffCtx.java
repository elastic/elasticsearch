package graphql.schema.diff;

import graphql.Internal;
import graphql.language.Document;
import graphql.language.Type;
import graphql.language.TypeDefinition;
import graphql.schema.diff.reporting.DifferenceReporter;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Stack;

/*
 * A helper class that represents diff state (eg visited types) as well as helpers
 */
@Internal
class DiffCtx {
    final List<String> examinedTypes = new ArrayList<>();
    final Deque<String> currentTypes = new ArrayDeque<>();
    private final DifferenceReporter reporter;
    final Document oldDoc;
    final Document newDoc;

    DiffCtx(DifferenceReporter reporter, Document oldDoc, Document newDoc) {
        this.reporter = reporter;
        this.oldDoc = oldDoc;
        this.newDoc = newDoc;
    }

    void report(DiffEvent differenceEvent) {
        reporter.report(differenceEvent);
    }

    boolean examiningType(String typeName) {
        if (examinedTypes.contains(typeName)) {
            return true;
        }
        examinedTypes.add(typeName);
        currentTypes.push(typeName);
        return false;
    }

    void exitType() {
        currentTypes.pop();
    }

    <T extends TypeDefinition> Optional<T> getOldTypeDef(Type type, Class<T> typeDefClass) {
        return getType(SchemaDiff.getTypeName(type), typeDefClass, oldDoc);
    }

    <T extends TypeDefinition> Optional<T> getNewTypeDef(Type type, Class<T> typeDefClass) {
        return getType(SchemaDiff.getTypeName(type), typeDefClass, newDoc);
    }

    private <T extends TypeDefinition> Optional<T> getType(String typeName, Class<T> typeDefClass, Document doc) {
        if (typeName == null) {
            return Optional.empty();
        }
        return doc.getDefinitions().stream()
                .filter(def -> typeDefClass.isAssignableFrom(def.getClass()))
                .map(typeDefClass::cast)
                .filter(defT -> defT.getName().equals(typeName))
                .findFirst();
    }
}
