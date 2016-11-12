fromStream("TweetStream1").when({
    $init : function(s, e){
        // if (state.items == null) return {items : new Array(), count : 0};
        // s.arr = new Array();
        // s.count = 0 ;
        return {arr : [], count : 0};
    },
    $any : function(s,e){
        if(e !== null && typeof(e.body) != "undefined" && e.body !== null && e.body.hasOwnProperty("user")){
            if(e.body.user.location!== null){
                s.arr.push(e.body.user.location);
                hashtagsArr = e.body.entities.hashtags;
                hashtags = {}
                for(var item in hashtagsArr){
                    hashtags[hashtagsArr[item].text] = 1
                }
                linkTo(e.body.user.location, e);
                emit(e.body.user.location,"sample", hashtags);
                s.count++;
            }
        }else{
            // emit("karthik1", e.body);
        }
        return s;
    }   
}); 