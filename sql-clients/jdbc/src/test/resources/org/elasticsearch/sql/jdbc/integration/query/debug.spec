//
// Spec used for debugging a certain test (without having to alter the spec suite of which it might be part of)
//

debug
SELECT emp_no, ABS(emp_no) m, first_name FROM "emp.emp" WHERE ABS(emp_no) < 10010 ORDER BY ABS(emp_no);