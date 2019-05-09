baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)
date=$1
configdir=$2
province=('811' '812' '813' '814' '815' '821' '822' '823' '831' '832' '833' '834' '835' '836' '837' '841' '842' '843' '844' '845' '846' '850' '851' '852' '853' '854' '861' '862' '863' '864' '865')
time=('00' '01' '02' '03' '04' '05' '06' '07' '08' '09' '10' '11' '12' '13' '14' '15' '16' '17' '18' '19' '20' '21' '22' '23')
runJar () {
       if [ $# -ne 4 ];then
         echo "Usage: require 4 parameter"
         return
       fi
       hadoop jar $baseDirForScriptSelf/PreprocessMrFocus.jar \
               com.tjdata.mr.main.PreprocessFocusMain \
               $1 $2 $3 $4 $5
}
for ((i=0; i<`echo ${#province[*]}`; i++))
        do
        for ((j=0; j<`echo ${#time[*]}`; j++))
                do
        echo prov=${province[$i]},date=${date},time=${time[$j]},flag=0,configdir=${configdir}
        runJar ${province[$i]} ${date} ${time[$j]} 0 ${configdir}
        done
done
runJar 0 0 0 1 ${configdir}