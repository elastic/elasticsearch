package graphql.parser;

import graphql.Internal;
import graphql.language.SourceLocation;
import org.antlr.v4.runtime.Token;

@Internal
public class SourceLocationHelper {

    public static SourceLocation mkSourceLocation(MultiSourceReader multiSourceReader, Token token) {
        //
        // multi source reader lines are 0 based while Antler lines are 1's based
        //
        // Antler columns ironically are 0 based - go figure!
        //
        int tokenLine = token.getLine() - 1;
        MultiSourceReader.SourceAndLine sourceAndLine = multiSourceReader.getSourceAndLineFromOverallLine(tokenLine);
        //
        // graphql spec says line numbers and columns start at 1
        int line = sourceAndLine.getLine() + 1;
        int column = token.getCharPositionInLine() + 1;
        return new SourceLocation(line, column, sourceAndLine.getSourceName());
    }

}
