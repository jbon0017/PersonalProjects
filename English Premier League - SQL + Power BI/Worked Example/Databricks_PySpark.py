# coding: utf-8

# In[11]:

import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from pandas import DataFrame
import json

homeurl = "https://www.soccerbase.com/referees/home.sd"

seasonurl = 'https://www.soccerbase.com/referees/home.sd?tourn_id='

text = requests.get(homeurl)
soup = BeautifulSoup(text.content, 'html.parser')

list_of_rows=[]

data = soup.findAll('div',attrs={'class':'seasonSelector clearfix'})
for div in data:
    links = div.findAll('option')
    for a in links:
        
#https://stackoverflow.com/questions/8616928/python-getting-all-links-from-a-div-having-a-class

        page = requests.get(seasonurl + a['value'])
        
        soup = BeautifulSoup(page.content, 'html.parser')
        
        table=soup.find('table', attrs={'class':'table referee'})

        for row in table.findAll('tr')[1:]:
            list_of_cells=[]
            for cell in row.findAll('td'):
                list_of_cells.append(cell.text)
            list_of_rows.append(list_of_cells)
            
df = DataFrame (list_of_rows,columns=['Referee Name','Referee Origin','Total Games','Yellow Cards','Red Cards'])

refereedf = DataFrame(df["Referee Name"].unique(),columns=['Referee Name'])
display(refereedf)
        
        #https://codebeautify.org/xmlviewer


# In[ ]:


#https://www.premierleague.com/referees/index
    
import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from pandas import DataFrame
import json


homeurl = "https://www.premierleague.com/referees/index"

seasonurl = 'https://www.soccerbase.com/referees/home.sd?tourn_id='


text = requests.get(homeurl)
soup = BeautifulSoup(text.content, 'html.parser')

list_of_rows=[]

data = soup.findAll('div',attrs={'class':'seasonSelector clearfix'})
for div in data:
    links = div.findAll('option')
    for a in links:
        
#https://stackoverflow.com/questions/8616928/python-getting-all-links-from-a-div-having-a-class

        page = requests.get(seasonurl + a['value'])
        
        soup = BeautifulSoup(page.content, 'html.parser')
        
        table=soup.find('table', attrs={'class':'table referee'})

        for row in table.findAll('tr')[1:]:
            list_of_cells=[]
            for cell in row.findAll('td'):
                list_of_cells.append(cell.text)
            list_of_rows.append(list_of_cells)
            
df = DataFrame (list_of_rows,columns=['Referee Name','Referee Origin','Total Games','Yellow Cards','Red Cards'])

refereedf = DataFrame(df["Referee Name"].unique(),columns=['Referee Name'])
display(refereedf)
        
        #https://codebeautify.org/xmlviewer


# In[15]:


import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from pandas import DataFrame
import json


homeurl = "https://www.worldfootball.net/referees/eng-premier-league-2000-2001/1/"

text = requests.get(homeurl)
soup = BeautifulSoup(text.content, 'html.parser')

print(soup)


# In[19]:


#https://www.premierleague.com/referees/index
    
import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from pandas import DataFrame
import json


homeurl = "https://www.worldfootball.net/referees/eng-premier-league-2000-2001/1/"

seasonurl = 'https://www.worldfootball.net/referees/'


text = requests.get(homeurl)
soup = BeautifulSoup(text.content, 'html.parser')

list_of_rows=[]

data = soup.findAll('div',attrs={'class':'data'})
for div in data:
    links = div.findAll('option')
    for a in links:
        
#https://stackoverflow.com/questions/8616928/python-getting-all-links-from-a-div-having-a-class

        page = requests.get(seasonurl + a['value'])
        
        soup = BeautifulSoup(page.content, 'html.parser')
        
        table=soup.find('table', attrs={'class':'standard_tabelle'})

        for row in table.findAll('tr')[1:]:
            list_of_cells=[]
            for cell in row.findAll('td'):
                list_of_cells.append(cell.text)
            list_of_rows.append(list_of_cells)
            
df = DataFrame (list_of_rows,columns=['Name','Born','Country','Matches','Yellow','DoubleYellow','Red','Other'])

refereedf = DataFrame(df["Name"].unique(),columns=['Name'])
display(refereedf)
        
        #https://codebeautify.org/xmlviewer


# In[24]:


#https://www.premierleague.com/referees/index
    
import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from pandas import DataFrame
import json


homeurl = "https://www.worldfootball.net/referees/eng-premier-league-2000-2001/1/"

seasonurl = 'https://www.worldfootball.net/referees/'

minimumurlsuffix = '/referees/eng-premier-league-2004-2005/1/'


text = requests.get(homeurl)
soup = BeautifulSoup(text.content, 'html.parser')

list_of_rows=[]

data = soup.findAll('div',attrs={'class':'data'})
for div in data:
    links = div.findAll('option')
    for a in links:
        if a['value'] >= minimumurlsuffix:
                    
#https://stackoverflow.com/questions/8616928/python-getting-all-links-from-a-div-having-a-class

            page = requests.get(seasonurl + a['value'])
            
            soup = BeautifulSoup(page.content, 'html.parser')
            
            table=soup.find('table', attrs={'class':'standard_tabelle'})
    
            for row in table.findAll('tr')[1:]:
                list_of_cells=[]
                for cell in row.findAll('td'):
                    list_of_cells.append(cell.text)
                list_of_rows.append(list_of_cells)
            
df = DataFrame (list_of_rows,columns=['Name','Born','Country','Matches','Yellow','DoubleYellow','Red','Other'])

refereedf = DataFrame(df["Name"].unique(),columns=['Name'])
display(refereedf)

