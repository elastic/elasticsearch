package graphql.schema.diff.reporting;

import graphql.PublicApi;
import graphql.schema.diff.DiffEvent;
import graphql.schema.diff.DiffLevel;

import java.io.PrintStream;

import static java.lang.String.format;

/**
 * A reporter that prints its output to a PrintStream
 */
@PublicApi
public class PrintStreamReporter implements DifferenceReporter {

    int breakageCount = 0;
    int dangerCount = 0;
    final PrintStream out;

    public PrintStreamReporter() {
        this(System.out);
    }

    public PrintStreamReporter(PrintStream out) {
        this.out = out;
    }

    @Override
    public void report(DiffEvent differenceEvent) {
        if (differenceEvent.getLevel() == DiffLevel.BREAKING) {
            breakageCount++;
        }
        if (differenceEvent.getLevel() == DiffLevel.DANGEROUS) {
            dangerCount++;
        }

        printEvent(differenceEvent);
    }

    private void printEvent(DiffEvent event) {
        String indent = event.getLevel() == DiffLevel.INFO ? "\t" : "";
        String level = event.getLevel() == DiffLevel.INFO ? "info" : event.getLevel().toString();
        String objectName = event.getTypeName();
        if (event.getFieldName() != null) {
            objectName = objectName + "." + event.getFieldName();
        }
        out.println(format(
                "%s%s - '%s' : '%s' : %s",
                indent, level, event.getTypeKind(), objectName, event.getReasonMsg()));
    }

    @Override
    public void onEnd() {
        out.println("\n");
        out.println(format("%d errors", breakageCount));
        out.println(format("%d warnings", dangerCount));
        out.println("\n");
    }
}
