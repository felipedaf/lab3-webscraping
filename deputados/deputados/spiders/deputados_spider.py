import scrapy
from decimal import Decimal

class DeputadosSpider(scrapy.Spider):
  name = "deputados"

  def start_requests(self):
    file_names = [('lista_deputados.txt', { 'sex': 'M' })]

    for (file_name, meta) in file_names:
      with open(file_name) as file:
        urls = file.read().split('\n')
      
      for url in urls:
        yield scrapy.Request(url=url, callback=self.parse, meta=meta)

  def parse(self, response):
    data = {}
    data['genero'] = response.meta['sex']

    target_keys = {
      'Nome Civil': 'nome',
      'Data de Nascimento': 'data_nascimento'
    }
    keys_informacoes_deputado = response.css('.informacoes-deputado > li > span::text').getall()
    values_informacoes_deputado = response.css('.informacoes-deputado > li::text').getall()

    for (index, key) in enumerate(keys_informacoes_deputado):
      formatted_key = key.strip().replace(':', '')
      if formatted_key in target_keys:
        data[target_keys[formatted_key]] = values_informacoes_deputado[index].strip()

    target_plenario_keys = {
      'Presenças': 'presença_plenario',
      'Ausências justificadas': 'ausencia_justificada_plenario',
      'Ausências não justificadas': 'ausencia_plenario',
    }
    keys_plenario_infos = response.css('.list-table__item:first-child > .list-table__definition-list > .list-table__definition-term::text').getall()
    values_plenario_infos = response.css('.list-table__item:first-child > .list-table__definition-list > .list-table__definition-description::text').getall()

    for (index, key) in enumerate(keys_plenario_infos):
      formatted_key = key.strip()
      if formatted_key in target_plenario_keys:
        data[target_plenario_keys[formatted_key]] = int(values_plenario_infos[index].strip().split(' ')[0])
    
    target_comissoes_keys = {
      'Presenças': 'presenca_comissao',
      'Ausências justificadas': 'ausencia_justificada_comissao',
      'Ausências não justificadas': 'ausencia_comissao',
    }
    keys_comissoes_infos = response.css('.list-table__item:nth-child(2) > .list-table__definition-list > .list-table__definition-term::text').getall()
    values_comissoes_infos = response.css('.list-table__item:nth-child(2) > .list-table__definition-list > .list-table__definition-description::text').getall()
    
    for (index, key) in enumerate(keys_comissoes_infos):
      formatted_key = key.strip()
      if formatted_key in target_comissoes_keys:
        data[target_comissoes_keys[formatted_key]] = int(values_comissoes_infos[index].strip().split(' ')[0])

    target_recursos_infos = {
      'Salário mensal bruto': 'salario_bruto_par',
      'Viagens em missão oficial': 'quant_viagem',
    }
    keys_recursos_infos = response.css('.recursos-deputado > .recursos-beneficios-deputado-container > li > .beneficio > .beneficio__titulo::text').getall()
    values_recursos_infos = response.css('.recursos-deputado > .recursos-beneficios-deputado-container > li > .beneficio > .beneficio__info::text').getall()
    
    while len(values_recursos_infos) > 6:
      values_recursos_infos.pop(3)

    for (index, key) in enumerate(keys_recursos_infos):
      formatted_key = key.strip()
      if formatted_key in target_recursos_infos:
        data[target_recursos_infos[formatted_key]] = values_recursos_infos[index].replace('\n', '').replace('R$', '').strip()
    
    proximas_paginas = response.css('.gasto .veja-mais a::attr(href)').getall()
    cota_parlamentar = proximas_paginas[0]
    verba_gabinete = proximas_paginas[1]

    yield scrapy.Request(cota_parlamentar, callback=self.parse_parlamentar, cb_kwargs={ 'data': data }, meta={ 'verba_gabinete_url': verba_gabinete })
  
  def parse_parlamentar(self, response, data):
    despesas = [price.strip().split()[1] for price in response.css('td.numerico::text').getall()]
    data['gasto_total_par'] = despesas[0]

    despesas = despesas[1:len(despesas) - 1]
    keys = ['gasto_jan_par', 'gasto_fev_par', 'gasto_mar_par', 'gasto_abr_par', 'gasto_maio_par', 'gasto_junho_par', 'gasto_jul_par', 'gasto_agosto_par', 'gasto_set_par', 'gasto_out_par', 'gasto_nov_par', 'gasto_dez_par']
    
    for key in keys:
      data[key] = '0'

    for (index, value) in enumerate(despesas):
      data[keys[index]] = value
    
    print(data)

    yield response.follow(response.meta['verba_gabinete_url'], callback=self.parse_gabinete, cb_kwargs={ 'data': data })
  
  def parse_gabinete(self, response, data):
    keys = ['gasto_jan_gab', 'gasto_fev_gab', 'gasto_mar_gab', 'gasto_abr_gab', 'gasto_maio_gab', 'gasto_junho_gab', 'gasto_jul_gab', 'gasto_agosto_gab', 'gasto_set_gab', 'gasto_out_gab', 'gasto_nov_gab', 'gasto_dez_gab']

    for key in keys:
      data[key] = '0'
    
    despesas = response.css('td.alinhar-direita:nth-child(3)::text').getall()
    despesas = list(map(lambda x: float(Decimal(x.replace('.', '', x.count('.')).replace(',', '.'))), despesas))

    data['gasto_total_gab'] = sum(despesas)

    for (index, value) in enumerate(despesas):
      data[keys[index]] = value

    yield data
