import re
import pandas as pd
import os
 
def generalizedTicker(filelist,textpath,pdfpath,dataframe,bracket=False,reversal_flag=False,ignorecase=False,reverse=False,open_leftpdfs=False,lookahead=0,noSearchTerm=True,searchTerm=None,limit=None,save_to_dataframe=False,print_item=True,print_filename=False,pattern=None,tickerProvider=True,select_item=0,tickerProviderName='Unknown Source',sep=':'):
    ''' A general function which can aid in finding the ticker symbol.
        
        Parameters-
            filelist: A list containing filename.
            (type=list)
            textpath: The directory path which conatins text files. 
            (type=str)
            pdfpath: The directory path which conatins pdf files. 
            (type=str)
            dataframe: The dataframe where values need to be stored. 
            (type=pd.DataFrame)
            bracket: Whether to include or exclude brackets in regex.
            (type=bool, default=False)
            ignorecase: Whether to ignore case while checking or not.
            (type=bool, default=False)
            reverse: Whether to reverse the way line is read or not. 
            (type=bool, default=False)
            open_leftpdfs: Whether to open pdf files in left(Not successfully parsed). 
            (type=bool, default=False)
            lookahead: The lines need to see further.  
            (type: int, default=0)
            noSearchTerm: Extra search term is present or not. 
            (type=bool, default=True)
            searchTerm: Search term. 
            (type=str, default=None)
            limit: The lines range to search if search term is provided. 
            (type:tuple, default=None)
            save_to_dataframe: Whether to save directly to dataframe or not. 
            (type=bool, default=False)
            print_item: Whether to print found ticker or not. 
            (type=bool, default=True)
            print_filename: Whether to print filename of successfully parsed file or not. 
            (type=bool, default=False)
            tickerProvider: Whether ticker provider is present or not. 
            (type=bool,default=True)

        valid if tickerProvider==False
            select_item: The position to select if item have multiple elements. 
            (type=int, default=0)
            tickerProviderName: The tickerProvider to which ticker belongs. 
            (type:str, default='Unknown Source')
         
        valid if tickerProvider==True
            reversal_flag: Position of ticker and tiickerProvider is reversed or not.
            (type=bool, default=False) 
            sep: The seprator used between ticker and ticker provider. 
            (type=str, default=':')
        
        Pattern-
         By default 'TICKER' kind of pattern is used.

        Return-
            a tuple:
                tuple[0] modified dataframe.
                tuple[1] conatains all filenames which are successfully parsed.
                tuple[2] conatains all filenames which are not successfully parsed.
        
        Future Scope-
            reArg: re findall arguments.Format: reArg=[pattern,,reversal_flag] 
            (type=list, default=['',''])
    '''
    done=[]
    i=0
    if tickerProvider:
        tickerProviderNames=[]
        if pattern==None:
            if reversal_flag:
                if bracket:
                    pattern=f'\(([A-Z]+[-]?[\.]?[A-Z]*)[ ]*{sep}[ ]*([A-Z]+).*\)'
                else:
                    pattern=f'([A-Z]+[-]?[\.]?[A-Z]*)[ ]*{sep}[ ]*([A-Z]+)'                
            else:    
                if bracket:
                    pattern=f'\(([A-Z]+)[ ]*{sep}[ ]*([A-Z]+[-]?[\.]?[A-Z]*).*\)'
                else:
                    pattern=f'([A-Z]+)[ ]*{sep}[ ]*([A-Z]+[-]?[\.]?[A-Z]*)'
    else:
        if pattern==None:
            if bracket:
                pattern='\(([A-Z]+[-]?[\.]?[A-Z]*)[ ]?[A-Z]*\)'
            else:
                pattern='[ ]*([A-Z]+[-]?[\.]?[A-Z]*)[ ]+'
    for filename in filelist:
        with open(textpath+'\\'+filename+'.txt',encoding='utf8') as fh:
            lines=fh.readlines()
        if noSearchTerm:
            limit=(0,len(lines))
        if reverse:
            lines=lines[::-1]
        for index,line in enumerate(lines):
            try:
                if noSearchTerm or line.find(searchTerm)!=-1:
                    j=limit[0]-1
                    if lookahead!=0:
                        j=lookahead-1
                    while True:
                        if j>=limit[1]:
                            break
                        else:
                            j+=1
                        if ignorecase:
                            items=re.findall(pattern,lines[index+j].upper().strip())
                        else:
                            items=re.findall(pattern,lines[index+j].strip())
                        if len(items)>0:
                            found_flag=True
                            if tickerProvider:
                                for item in items:
                                    tickerProviderName=item[0]
                                    ticker=item[1]
                                    if tickerProviderName in ['NYSE','NASDAQ','Bloomberg','Reuters','NAS','BBG']:
                                        if tickerProviderName=='NAS':
                                            tickerProviderName='NASDAQ'
                                        if tickerProviderName=='BBG':
                                            tickerProviderName='Bloomberg'
                                    elif ticker in ['NYSE','NASDAQ','Bloomberg','Reuters','NAS','BBG']:
                                        temp=ticker
                                        ticker=tickerProviderName
                                        tickerProviderName=temp
                                        if tickerProviderName=='NAS':
                                            tickerProviderName='NASDAQ'
                                        if tickerProviderName=='BBG':
                                            tickerProviderName='Bloomberg'
                                        reversal_flag=True           #so that in future places of ticker and ticker provider is excanged
                                    else:
                                        if reversal_flag:
                                            temp=ticker
                                            ticker=tickerProviderName
                                            tickerProviderName=temp
                                        tickerProviderName='Unknown Source'
                                    if print_item:
                                        print(f'{tickerProviderName} : {ticker}')
                                    if ticker.isupper()==False:
                                        found_flag=False
                                        continue
                                    if save_to_dataframe:
                                        dataframe.loc[filename,tickerProviderName]=ticker
                                    tickerProviderNames.append(tickerProviderName)
                            else:
                                if print_item:    
                                    print(items)
                                ticker=items[select_item]
                                if ticker.isupper()==False:
                                    found_flag=False
                                    continue
                                if save_to_dataframe:
                                    dataframe.loc[filename,tickerProviderName]=ticker
                            if found_flag:
                                i+=1
                                if print_filename:
                                    print(filename)
                                done.append(filename)
                                raise BaseException
            except BaseException:
                break
 
    if save_to_dataframe:
        if tickerProvider:
            viewlist=['Client']+list(set(tickerProviderNames))
            print(dataframe.loc[done,viewlist])
        else:
            print(dataframe.loc[done,['Client',tickerProviderName]])        
    left=[filename for filename in filelist if filename not in done]
    if open_leftpdfs:
        for filename in left:
            os.startfile(pdfpath+'\\'+filename+'.pdf')
    print(f"{i} files successfully passed.")
    return dataframe,done,left