 "Starting script ..."

[ -s /mr-excl/mr-excl/src/output ]
if [[ $? == 0 ]]; then
	ls -lrt /mr-excl/mr/excl/src/output
	echo "Cleaning up directory /mr-excl/output"
	rm -r -f /mr-excl/mr-excl/src/output
	echo "Return code $?"

fi


echo "Compiling program exclusions.java..."
javac -cp $(hadoop classpath) /mr-excl/mr-excl/src/exclusions.java -d /mr-excl/mr-excl/src/exclusionsClasses
if [[ $? == 1 ]]; then
  echo " Compile failed!"
  exit  1
fi


echo "Creating jar file from the class binaries..."
jar -cvf /mr-excl/mr-excl/src/exclusions.jar -C /mr-excl/mr-excl/src/exclusionsClasses/ ./
if [[ $? == 1 ]]; then
  echo " jar creation failed!"
  exit 1
fi

echo "Running hadoop program"
hadoop jar exclusions.jar exclusions
if [[ $? == 1 ]]; then
  echo  "running hadoop mr job failed!"
  exit 1
fi

echo "All-good, Exiting master script"

ls -lrt /mr-excl/mr-excl/src/output



