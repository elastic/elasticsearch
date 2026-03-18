# Mapped and unmapped
[x] 2 indices, a field is mapped in one to a keyword, and in another is unmapped; No casts. (fieldIsPartiallyUnmappedMultiIndex)
[x] 2 indices, a field is mapped in one to a keyword, and in another is unmapped; cast to keyword (typeConflictKeywordUnmappedCastToKeyword)
[x] 2 indices, a field is mapped in one to a long, and in another is unmapped; cast to keyword (typeConflictLongUnmappedCastToKeyword)
[x] 2 indices, a field is mapped in one to a long, and in another is unmapped; cast to long (typeConflictLongUnmappedCastToLong)
[x] 2 indices, a field is mapped in one to a long, and in another is unmapped; cast to double (typeConflictLongUnmappedCastToDouble)

# mapped and non-existent
[x] 2 indices, a field is mapped in one to a keyword, and in another doesn't exist; No casts. (typeConflictKeywordNonExistentNoCast)
[x] 2 indices, a field is mapped in one to a keyword, and in another doesn't exist; cast to keyword (typeConflictKeywordNonExistentCastToKeyword)
[x] 2 indices, a field is mapped in one to a long, and in another doesn't exist; cast to keyword (typeConflictLongNonExistentCastToKeyword)
[x] 2 indices, a field is mapped in one to a long, and in another doesn't exist; cast to long (typeConflictLongNonExistentCastToLong)
[x] 2 indices, a field is mapped in one to a long, and in another doesn't exist; cast to double (typeConflictLongNonExistentCastToDouble)

# mapped x 2 and unmapped
[x] 3 indices, field is mapped to long in one, date in another, and unmapped in a third; Cast to keyword (typeConflictLongDateUnmappedCastToKeyword)
[x] 3 indices, field is mapped to long in two, unmapped in a third; Cast to double (typeConflictLongDateUnmappedCastToDouble; uses event_duration to avoid date-string parse warnings)
[x] 3 indices, field is mapped to long in one, date in another, and unmapped in a third; Cast to date (typeConflictLongDateUnmappedCastToDate)

# mapped x 2 and non-existent
[x] 3 indices, field is mapped to long in one, date in another, and non-existant in a third; Cast to keyword (typeConflictLongDateNonExistentCastToKeyword)
[x] 3 indices, field is mapped to long in one, date in another, and non-existant in a third; Cast to double (typeConflictLongDateNonExistentCastToDouble)
[x] 3 indices, field is mapped to long in one, date in another, and non-existant in a third; Cast to date (typeConflictLongDateNonExistentCastToDate)

# mapped x 2, unmapped non-existent
[x] 4 indices, field is mapped to long in one, date in another, unmapped in a third, and doesn't exist in a 4th; Cast to keyword (typeConflictLongDateUnmappedNonExistentCastToKeyword)
[x] 4 indices, field is mapped to long in one, date in another, unmapped in a third, and doesn't exist in a 4th; Cast to double (typeConflictLongDateUnmappedNonExistentCastToDouble)
[x] 4 indices, field is mapped to long in one, date in another, unmapped in a third, and doesn't exist in a 4th; Cast to date (typeConflictLongDateUnmappedNonExistentCastToDate)

# failures (In the AnalyzerUnmappedTests, i.e., non-golden)
[x] 2 indices, a field is mapped in one to long, and in another is unmapped; No casts (testTypeConflictLongUnmappedNoCast)
[x] 3 indices, a field is mapped in one to long, keyword in another, and unmapped in a third; no casts (testTypeConflictLongKeywordUnmappedNoCast)
[x] 3 indices, a field is mapped in one to long, int in another, and unmapped in a third; no casts (testTypeConflictLongIntUnmappedNoCast)
[x] 2 indices, a field is mapped to text in one and unmapped in another; no casts (testTypeConflictTextUnmappedNoCast)


