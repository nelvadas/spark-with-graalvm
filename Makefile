large.txt: lorem.txt
	for i in $(shell seq 3000); do cat $< >> $@; done
	cp $@ temp.txt
	for i in $(shell seq 59); do cat temp.txt  >> $@; done

clean:
	rm -f large.txt temp.txt
