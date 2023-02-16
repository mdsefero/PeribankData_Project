mkdir truncated
for f in Peribank*; do
	head -1000 $f > 'trc_'$f
done
mv trc* truncated/