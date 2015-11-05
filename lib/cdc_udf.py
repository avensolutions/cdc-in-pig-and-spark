@outputSchema('concatenated_field:chararray')
def concat_fields(in_tuple):
	retstr = ""
	for field in in_tuple:
		retstr = retstr + str(field)
	return retstr