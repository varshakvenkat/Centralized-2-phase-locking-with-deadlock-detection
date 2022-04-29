pwd=`pwd`

osascript -e 'tell application "Terminal" to do script "python3 -m Pyro4.naming"'
for i in $(eval echo "{1..$2}")
do
   osascript -e 'tell application "Terminal" to do script "cd \"'"$pwd"'\" ; python3 data_manager.py -n site'"$i"'"'
done
osascript -e 'tell application "Terminal" to do script "cd \"'"$pwd"'\" ; python3 lock_manager.py '"$2"'"'
for i in $(eval echo "{1..$2}")
do
   osascript -e 'tell application "Terminal" to do script "cd \"'"$pwd"'\" ; python3 transaction_manager.py -n site'"$i"' -l '$1' -ns '"$2"'"'
done